import logging, json, configparser, os

# Load config
config = configparser.ConfigParser()
config.read('config.ini')

VERBOSITY_LEVELS = ['LESS', 'MORE', 'MAX']
current_detail = config.get('logging', 'detail', fallback='LESS').upper()
log_file = config.get('logging', 'log_file', fallback='scraper.log')

# Event verbosity mapping
EVENT_VERBOSITY = {
    'job_start': 'LESS',
    'job_complete': 'LESS',
    'login_success': 'MORE',
    'login_failed': 'MORE',
    'api_request': 'MAX',
    'api_response': 'MAX',
    'bonus_fetched': 'MORE',
    'downline_fetched': 'MORE',
    'csv_written': 'MORE',
    'exception': 'LESS'
}

# JSON Formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "module": record.module,
            "method": record.funcName,
            "event": record.msg,
            "details": record.args if record.args else {}
        }
        return json.dumps(log_record)

# Logger setup
logger = logging.getLogger("ScraperLogger")
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(log_file)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

def emit_event(event_name, details, level="INFO"):
    required_level = EVENT_VERBOSITY.get(event_name, 'MORE')
    if VERBOSITY_LEVELS.index(current_detail) >= VERBOSITY_LEVELS.index(required_level):
        logger.log(getattr(logging, level), event_name, details)

def load_historical_metrics():
    metrics = {
        'bonuses': 0,
        'downlines': 0,
        'errors': 0,
        'runs': 0,
        'total_runtime': 0.0
    }
    if not os.path.exists(log_file):
        return metrics
    with open(log_file) as f:
        for line in f:
            try:
                log = json.loads(line)
                event = log.get("event")
                details = log.get("details", {})
                if event == "bonus_fetched":
                    metrics['bonuses'] += details.get('count', 0)
                elif event == "downline_fetched":
                    metrics['downlines'] += details.get('count', 0)
                elif event == "exception":
                    metrics['errors'] += 1
                elif event == "job_complete":
                    metrics['runs'] += 1
                    metrics['total_runtime'] += details.get('duration', 0.0)
            except:
                continue
    return metrics
