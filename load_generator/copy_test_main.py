import json
import base64
import mock

import main


mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'


def test_print_table(capsys):
    params = {
        'data_file': 'gs://workshop-datasets/covertype/small/dataset.csv',
        'num_examples': 3,
        'service_name': 'projects/mlops-dev-env/models/covertype_classifier_tf/versions/v2',
        'signature_name': 'serving_default',
        'model_output_key': 'output_1'
    }

    message = json.dumps(params)
    data = {'data': base64.b64encode(message.encode())}

    # Call tested function
    main.run_predictions(data, mock_context)
    #out, err = capsys.readouterr()
    #assert 'covertype_dataset.covertype\n' in out
    
