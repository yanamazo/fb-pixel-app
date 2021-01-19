from flask import request
from facebook_business.adobjects.serverside.content import Content
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.gender import Gender
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.serverside.normalize import Normalize
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.serverside.action_source import ActionSource
import json
import time


def fire_pixel_event(request):
    #Get FB Conversions API credentials from request headers
    ACCESS_TOKEN = request.headers.get('token')
    PIXEL_ID = request.headers.get('pixel')
    
    #Setup logs using logging library https://cloud.google.com/run/docs/logging
    PROJECT = '****'
    global_log_fields = {}
    trace_header = request.headers.get('X-Cloud-Trace-Context')
    trace = trace_header.split('/')
    global_log_fields['logging.googleapis.com/trace'] = (f"projects/{PROJECT}/traces/{trace[0]}")
    
    #Initialize FB Conversions API and setup data for request
    FacebookAdsApi.init(access_token=ACCESS_TOKEN)
    now = int(time.time())
    action_source = ActionSource('chat')
    payload = request.json
    test_event_code = payload['test_event_code']

    if payload['opt_out'].lower().strip() == 'false':
        opt_out = False
    else:
        opt_out = True

    user_data = payload['user_data']
    user_data_0 = UserData()
    for key, value in user_data.items():
        setattr(user_data_0, key, value)

    custom_data = payload['custom_data']
    if payload["event_name"] == 'Purchase':
        try:
            custom_data['value'] = float(custom_data['value'])
        except ValueError or TypeError:
            response = f'{custom_data["value"]} is not allowed as value for {payload["event_name"]} event'
            entry = dict(message={'page_id': page_id, 'log': response},
                    component='arbitrary-property',
                    **global_log_fields)
            print(json.dumps(entry))
            return response, 400
    if 'predicted_ltv' in custom_data.keys():
        custom_data['predicted_ltv'] = float(custom_data['predicted_ltv'])
    if 'num_items' in custom_data.keys():
        custom_data['num_items'] = int(custom_data['num_items'])
    custom_data_0 = CustomData()
    for key, value in custom_data.items():
        setattr(custom_data_0, key, value)

    event_0 = Event(
        event_name=payload['event_name'],
        event_time=now,
        opt_out=opt_out,
        user_data=user_data_0,
        custom_data=custom_data_0,
        action_source=action_source
    )

    events = [event_0]
    event_request = EventRequest(
        events=events,
        pixel_id=PIXEL_ID,
        test_event_code=test_event_code
    )

    try:
        event_response = event_request.execute()
    except (TypeError, AttributeError) as error:
        error_message = error.args[0]
        response = {'response': error_message}
        return response, 400
    except FacebookRequestError as error:
        error_message = error.body()['error']['message']
        response = {'response': error_message}
        return response, 400
    else:
        response = event_response.to_dict()
        return response
    finally:
        entry = dict(message={'page_id': page_id, 'log': response},
                    component='arbitrary-property',
                    **global_log_fields)
        print(json.dumps(entry))
