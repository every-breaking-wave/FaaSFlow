import logging
 
logging.basicConfig(level=logging.INFO,
                    format='[%(funcName)s] %(asctime)s - %(levelname)s - %(message)s')


logger = logging.getLogger(__name__)