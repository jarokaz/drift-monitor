import base64
import datetime
import json
import mock

from generate_predict_tasks import create_predict_task 

def test_create_predict_task(capsys):

    project = 'mlops-dev-env'
    location = 'us-central1'
    queue =  'caip-player'
    model_name = 'covertype_tf'
    model_version = 'v3'
    instances = [
            { 
                'Soil_Type': ['7202'],
                'Wilderness_Area': ['Commanche'],
                'Aspect': [61.0],
                'Elevation': [3091.1],
                'Hillshade_3pm': [129],
                'Hillshade_9am': [227],
                'Hillshade_Noon': [223],
                'Horizontal_Distance_To_Fire_Points': [2868],
                'Horizontal_Distance_To_Hydrology': [134],
                'Horizontal_Distance_To_Roadways': [0], 
                'Slope': [8], 
                'Vertical_Distance_To_Hydrology': [10],
            }]

    in_seconds = 10
    execute_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=in_seconds)
    service_account = "caipp-caller@mlops-dev-env.iam.gserviceaccount.com"

    response = create_predict_task(
        project=project,
        queue=queue,
        service_account=service_account,
        location=location,
        model_name=model_name,
        model_version=model_version,
        instances=instances,
        execute_time=execute_time
    )

    print(response)

    #out, err = capsys.readouterr()
    #assert 'covertype_dataset.covertype\n' in out
        
