from util.environment_loader import load_app_version

if __name__ != '__main__':
    exit()

from runner import Runner
import logging.config
import yaml

with open('log-config.yml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

logger.info('Starting Application with version %s ...' % load_app_version())
runner = Runner()
