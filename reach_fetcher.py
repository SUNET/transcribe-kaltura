import os
import time
import math
import sys
import logging
import argparse
import hashlib
import signal
import threading
import jwt
import json
import re
from datetime import datetime
from datetime import timedelta

import KalturaClient
from KalturaClient.Plugins import Core as KalturaCore
from KalturaClient.Plugins.Core import KalturaUrlResource, KalturaKeyValue, KalturaStringResource
from KalturaClient.Plugins import Reach as KalturaReach

from KalturaClient.exceptions import KalturaClientException
from KalturaClient.exceptions import KalturaException

from transcriber_client import TranscriberClient

import sentry_sdk

if __name__ == "__main__":
    level = logging.INFO
    if "-v" in sys.argv:
        level = logging.DEBUG
        logging.getLogger("urllib3").setLevel(logging.INFO)
    if "-vv" in sys.argv:
        level = logging.DEBUG
    logging.basicConfig(format='[%(levelname)-8s] %(message)s', level=level)  # %(name)s - To add logger name
    logger = logging.getLogger(__name__)

    sentrydsn = None
    if 'SENTRY_DSN' in os.environ:
        sentrydsn = os.environ['SENTRY_DSN']
    if sentrydsn:
        logger.info("Running with Sentry enabled")
    sentry_sdk.init(sentrydsn)


def signal_drain(sig, frame):
    global drain
    logger.info("Draining active tasks.")
    drain = True


def signal_resume(sig, frame):
    global drain
    logger.info("Resuming from drain state.")
    drain = False


def signal_handler(sig, frame):
    global event
    if event.is_set():
        sys.exit(-1)
    else:
        if sig == signal.SIGINT:
            print('Received CTRL+C, press again to force...')
        print('Stopping...')
        event.set()


def loadargs():
    parser = argparse.ArgumentParser(description='Fetch new REACH requests from Kaltura.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-wid', '--workerid', metavar='WORKERID', help='Name of this worker', required=True)
    parser.add_argument('-pid', '--partner-id', metavar='PID', help='REACH Partner ID', required=True)
    parser.add_argument('-kurl', '--kaltura-url', metavar='SERVICEURL', help='URL for Kaltura service', required=True)
    parser.add_argument('-murl', '--middleware-url', metavar='MIDDLEWAREURL', help='URL for transcriber service', required=True)
    parser.add_argument('-ktid', '--kaltura-token-id', metavar='KALTURA-TOKEN-ID', help='Kaltura token ID', required=True)
    parser.add_argument('-c', '--config-file', metavar='CONFIG-FILE', help='Config file', required=False, default='config.json')
    parser.add_argument('-s', '--sleep', help='Sleep time in seconds', type=int, default=30)
    verbose_group = parser.add_mutually_exclusive_group()
    verbose_group.add_argument("-v", help="Verbose output", action="store_true")
    verbose_group.add_argument("-vv", help="Extra Verbose output (output HTTP requests)", action="store_true")
    args = parser.parse_args()

    if 'KALTURAPARTNERSECRET' not in os.environ:
        print("Must set environment variable KALTURAPARTNERSECRET.", file=sys.stderr)
        print("Keep this value secret! Avoid bash_history and similar!", file=sys.stderr)
        raise ValueError("KALTURAPARTNERSECRET must be set")

    if 'TRANSCRIBERTOKENSECRET' not in os.environ:
        print("Must set environment variable TRANSCRIBERTOKENSECRET.", file=sys.stderr)
        print("Keep this value secret! Avoid bash_history and similar!", file=sys.stderr)
        raise ValueError("TRANSCRIBERTOKENSECRET must be set")

    return args


def getKalturaClient(args, ks=None):
    conf = KalturaClient.KalturaConfiguration(serviceUrl=args.kaltura_url)
    client = KalturaClient.KalturaClient(conf)
    client.clientConfiguration['clientTag'] = 'transcriber_adapter'

    if not ks:
        # logger.debug('Using appToken')
        timestamp = int((datetime.now() + timedelta(days=1)).timestamp())
        wsession = client.session.startWidgetSession(f'_{args.partner_id}', timestamp)
        wclient = KalturaClient.KalturaClient(conf)
        wclient.setKs(wsession.ks)

        tokenHash = hashlib.sha256((wsession.ks + os.environ['KALTURAPARTNERSECRET']).encode('ascii')).hexdigest()
        print("Kaltura Partner Secret: " + os.environ['KALTURAPARTNERSECRET'])
        print("taken Hash: " + tokenHash)
        res = wclient.appToken.startSession(args.kaltura_token_id, tokenHash, '', KalturaCore.KalturaSessionType.ADMIN, timestamp, '')
        ks = res.ks
        wclient.setKs(ks)
        token = wclient.appToken.get(args.kaltura_token_id)
        if token.expiry:
            expiry = datetime.fromtimestamp(token.expiry)
            diff = expiry - datetime.now()
            if diff.days < 7:
                logger.warning('Kaltura token expires in less than a week.')
                sentry_sdk.capture_message('Kaltura token expires in less than a week.')
            elif diff.days < 30:
                logger.warning('Kaltura token expires in less than a month.')
                sentry_sdk.capture_message('Kaltura token expires in less than a month.')

    # logger.debug('KS: %s', ks)

    client.setKs(ks)

    return client


def setKalturaError(kalClient, taskId, message, transcriberId=None):
    logger.error('Setting error state: %i - %s - %s', taskId, message, transcriberId)
    vendorTask = KalturaReach.KalturaEntryVendorTask()
    vendorTask.status = KalturaReach.KalturaEntryVendorTaskStatus.ERROR
    vendorTask.errDescription = message
    kalClient.reach.entryVendorTask.updateJob(taskId, vendorTask)


def getModel(partnerId, sourceLanguage, conf=None):
    global config
    if not conf:
        conf = config
    model = conf['default_model']
    model = conf['language_override'].get(sourceLanguage, model)
    model = conf['partner_override'].get(partnerId, {}).get(sourceLanguage, model)
    return model


def handlePending(task, entryClient, transcriberClient, kalClient):
    logger.debug(f'Handling pending task: {task.id}')

    # entry = entryClient.media.get(task.entryId)
    # logger.debug('Name: %s', entry.name)

    catalogItem = kalClient.reach.vendorCatalogItem.get(task.getCatalogItemId())
    sourceLanguage = catalogItem.getSourceLanguage().getValue()
    logger.debug('Source language: %s', sourceLanguage)

    assetFilter = KalturaCore.KalturaFlavorAssetFilter()
    assetFilter.entryIdEqual = task.entryId
    assetFilter.statusEqual = KalturaReach.KalturaFlavorAssetStatus.READY
    flavors = entryClient.flavorAsset.list(assetFilter)

    logger.debug('Flavor count: %i', flavors.totalCount)

    size = math.inf
    flavorId = None
    for flavor in flavors.objects:
        if flavor.fileExt not in ['mp4', 'mp3', '3gp']:
            logger.info('Skipping %s for flavor %s.', flavor.id, flavor.fileExt)
            continue
        logger.debug('Flavor: %s - %i', flavor.id, flavor.size)
        if flavor.size <= 0:
            continue
        if flavor.size < size:
            size = flavor.size
            flavorId = flavor.id

    if not flavorId:
        setKalturaError(kalClient, task.id, 'No flavor found')
        return
    logger.debug('Smallest flavor: %s', flavorId)

    url = entryClient.flavorAsset.getUrl(flavorId)
    url = re.sub('^https?://vod-cache', 'https://streaming', url, count=1)

    tasks = transcriberClient.get_tasks_by_ref_id([str(task.id)])
    logger.info(f"tasks: {tasks}")
    newTaskId = None
    skipUpload = False
    model = getModel(task.getPartnerId(), sourceLanguage)
    if len(tasks["result"]) > 0:
        logger.info("Task already exists for %s: %s", task.id, tasks["result"])
        newTaskId = tasks["result"]["id"]
        if tasks["result"]["status"] == 'processing':
            skipUpload = True
    else:
        newTask = {
            "prirority": "Medium",
            "model": model,
            # "model": "whisper_large_kb_se",
            "billingRef": str(task.id),
            "file_url": url,
            "language": sourceLanguage
        }
        newTaskId = transcriberClient.add_task(newTask)
        if newTaskId is None:
            setKalturaError(kalClient, task.id, 'Error adding new task')
            return

        logger.info("New Transcriber task for %s: %s", task.id, newTaskId)
        logger.info("Model for %s: %s", task.id, model)

    vendorTask = KalturaReach.KalturaEntryVendorTask()
    vendorTask.status = KalturaReach.KalturaEntryVendorTaskStatus.PROCESSING
    kalClient.reach.entryVendorTask.updateJob(task.id, vendorTask)

    logger.info('Successfully added: %i - %s - %s', task.id, newTaskId, task.getPartnerId())
    logger.info('URL: %s', url)


def handleProcessing(task, entryClient, transcriberClient, kalClient):
    logger.debug(f'Handling processing task: {task.id}')

    catalogItem = kalClient.reach.vendorCatalogItem.get(task.getCatalogItemId())
    sourceLanguage = catalogItem.getSourceLanguage().getValue()
    logger.debug('Source language: %s', sourceLanguage)

    tasks = transcriberClient.get_tasks_by_ref_id([str(task.id)])

    if tasks == None:
        return

    if len(tasks) > 1:
        logger.error("Multiple tasks found for %i: %s", task.id, str(tasks))
        # TODO Add error handling
    if len(tasks) < 1:
        logger.error("No task found for %i", task.id)

    # TODO: Error handling when no task returned
    transcriberTask = tasks["result"]

    if transcriberTask == {}:
        logger.debug("No task found in handleprocessing")
        return

    logger.debug("Task: {}".format(transcriberTask))

    logger.debug("Found task for %i: %s - %s", task.id, transcriberTask['id'], transcriberTask['status'])

    if transcriberTask['status'] == 'error':
        logger.error('Task failed: %i - %s', task.id, transcriberTask['id'])
        # TODO: send error
        vendorTask = KalturaReach.KalturaEntryVendorTask()
        vendorTask.status = KalturaReach.KalturaEntryVendorTaskStatus.ERROR
        vendorTask.errDescription = 'Task failed'
        kalClient.reach.entryVendorTask.updateJob(task.id, vendorTask)

        return

    if transcriberTask['status'] != 'completed':
        logger.debug('Task not ready.')
        return

    captionAsset = KalturaClient.Plugins.Caption.KalturaCaptionAsset()
    captionAsset.tags = "ndn-whisper"
    captionAsset.language = sourceLanguage
    captionAsset.label = sourceLanguage + " (Whisper)"
    captionAsset.accuracy = 90

    # FOR TESTING
    # captionAsset.fileExt = "vtt"
    # captionAsset.format = KalturaClient.Plugins.Caption.KalturaCaptionType.WEBVTT
    # url = "https://api.kaltura.nordu.net/content/entry/data/1/158/0_za9v61gb_0_be1wjj7p_2.vtt"

    captionId = entryClient.caption.captionAsset.add(task.entryId, captionAsset).id
    logger.debug('New caption ID: %s', captionId)

    builtUrl = transcriberClient.build_task_result_url(url)

    logger.debug("Built Url: {}".format(builtUrl))

    #urlResource = KalturaClient.Plugins.Core.KalturaUrlResource(url=builtUrl, urlHeaders=[KalturaKeyValue("x-client-dn", "Kaltura-adaptor")])

    stringResource = KalturaStringResource(content=transcriberTask["result_srt"])

    try:
        logger.info("setContent -  {} . {}".format(captionId, stringResource))
        entryClient.caption.captionAsset.setContent(captionId, stringResource)
        captionObj = entryClient.caption.captionAsset.get(captionId)
        retry = 0

        while captionObj.getStatus().getValue() != 2 and retry < 10:
            logger.debug('Captions not done...')
            time.sleep(retry)
            captionObj = entryClient.caption.captionAsset.get(captionId)
            retry += 1

        if captionObj.getStatus().getValue() != 2:
            logger.warning('Caption upload failed! %i', captionObj.getStatus().getValue())
            # TODO: Handle error

        vendorTask = KalturaReach.KalturaEntryVendorTask()
        vendorTask.status = KalturaReach.KalturaEntryVendorTaskStatus.READY
        vendorTask.outputObjectId = captionObj.getId()
        kalClient.reach.entryVendorTask.updateJob(task.id, vendorTask)
    except KalturaClient.exceptions.KalturaException as ex:
        if ex.code == "ENTRY_ID_NOT_FOUND":
            logger.info("Entry deleted: %s - %s", task.id, str(transcriberTask['id']))
            vendorTask = KalturaReach.KalturaEntryVendorTask()
            vendorTask.status = KalturaReach.KalturaEntryVendorTaskStatus.ERROR
            vendorTask.errDescription = 'Entry deleted'
            kalClient.reach.entryVendorTask.updateJob(task.id, vendorTask)
        else:
            raise ex

    logger.info('Files recieved: %i', task.id)

def checkToken(token):
    payload = jwt.decode(token, options={"verify_signature": False})
    exp = payload['exp']
    expiry = datetime.fromtimestamp(exp)
    diff = expiry - datetime.now()
    if diff.total_seconds() < 0:
        logger.error('Transcriber token has expired!')
        sentry_sdk.capture_message('Transcriber token has expired!')
        sys.exit(-1)
    if diff.days < 7:
        logger.warning('Transcriber token expires in less than a week.')
        sentry_sdk.capture_message('Transcriber token expires in less than a week.')
    elif diff.days < 30:
        logger.warning('Transcriber token expires in less than a month.')
        sentry_sdk.capture_message('Transcriber token expires in less than a month.')

    return

def getConfig(conf_file):
    config = {}
    if os.path.isfile(conf_file):
        config = json.load(open(conf_file))
        logger.info(f'Loading config from file: {conf_file}')
    else:
        logger.warning(f'Config file not found: {conf_file}')
    config['default_model'] = config.get('default_model', 'whisper_large_v3')
    config['language_override'] = config.get('language_override', {})
    config['language_override'] = config['language_override'] or {}
    config['partner_override'] = config.get('partner_override', {})
    config['partner_override'] = config['partner_override'] or {}
    return config


def run():
    global drain
    drain = False
    global event
    event = threading.Event()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGUSR1, signal_drain)
    signal.signal(signal.SIGUSR2, signal_resume)

    args = loadargs()

    global config
    config = getConfig(args.config_file)

    kalClient = getKalturaClient(args)
    kalClient.loadPlugin("Reach")

    checkToken(os.environ['TRANSCRIBERTOKENSECRET'])
    transcriberClient = TranscriberClient(args.middleware_url, os.environ['TRANSCRIBERTOKENSECRET'])

    while not event.is_set():

        filter = KalturaReach.KalturaEntryVendorTaskFilter()
        filter.statusIn = ','.join([str(KalturaReach.KalturaEntryVendorTaskStatus.PENDING), str(KalturaReach.KalturaEntryVendorTaskStatus.PROCESSING)])
        res = kalClient.reach.entryVendorTask.list(filter)

        # logger.debug('Total count: %i', res.totalCount)
        numProcessing = 0

        for task in res.objects:
            logger.info('Task: %i - %s - %s', task.id, task.status.value, task.entryId)

            entryClient = getKalturaClient(args, task.accessKey)

            match task.status.value:
                case KalturaReach.KalturaEntryVendorTaskStatus.PENDING:
                    if not drain:
                        handlePending(task, entryClient, transcriberClient, kalClient)
                case KalturaReach.KalturaEntryVendorTaskStatus.PROCESSING:
                    handleProcessing(task, entryClient, transcriberClient, kalClient)
                    numProcessing += 1
                case _:
                    logging.warning("Unknown Kaltura task status: {task.status}")
                    sentry_sdk.capture_message("Unknown Kaltura task status: {task.status}")
                    pass

        if numProcessing == 0 and drain:
            logger.info("Done draining, no active tasks")
            while drain and not event.is_set():
                event.wait(args.sleep)

        # logger.debug('Done handling tasks, sleeping...')
        event.wait(args.sleep)


if __name__ == "__main__":
    run()

    # Stop sentry
    sentry_sdk.Hub.current.client.close()
