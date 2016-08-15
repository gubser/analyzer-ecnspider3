from itertools import repeat
import json

from ptocore.analyzercontext import AnalyzerContext
from ptocore import sensitivity

from dateutil.parser import parse

def prepare(upload):
    """
    split upload into lines
    """
    filename, (metadata, data) = upload

    upload_id = metadata['_id']

    lines = data.split(b"\n")

    return zip(repeat(upload_id), lines)

def observations(record):
    """
    parse json line string and return observation
    """
    upload_id, line = record

    obj = json.loads(line.decode('utf-8'))

    return {
        'conditions': obj['conditions'],
        'time': {
            'from': parse(obj['time']['from']),
            'to': parse(obj['time']['to'])
        },
        'path': [obj['sip'], '*', obj['dip']],
        'value': {},
        'sources': {'upl': [upload_id]}
    }

def main():
    ac = AnalyzerContext()

    max_action_id, upload_ids = ac.action_set.direct()

    ac.set_result_info_direct(max_action_id, upload_ids)

    uploads = ac.spark_uploads_direct()

    uploads.flatMap(prepare).map(observations).saveToMongoDB(ac.temporary_uri)

if __name__ == "__main__":
    main()