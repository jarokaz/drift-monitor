# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" A Cloud Function that generates calls to AI Platform Prediction model """

import base64
import json
import os

import googleapiclient.discovery
import pandas as pd

from flask import escape


def run_predictions(request):
    """ HTTP Cloud Function that calls an AI Platform Predictions model.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    
    request_json = request.get_json(silent=True)
    request_args = request.args
     
    predictions = _call_caip_predict(
        params['service_name'], 
        params['signature_name'], 
        params['model_output_key'], 
        instances)
        
    return predictions



    
    