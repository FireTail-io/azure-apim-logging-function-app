import json
import requests
import time
import os
import logging
import azure.functions as func
import ndjson
from dateutil.parser import parse

app = func.FunctionApp()
session = requests.session()


def convert_datetime_to_seconds(datetime_str: str) -> int:
    try:
        dt = parse(datetime_str)
        return int(dt.timestamp() * 1000)
    except Exception:
        pass
    return int(time.time() * 1000)


def convert_headers_to_dict(headers):
    return_headers = {}
    first_split = headers.split(";;")
    for header in first_split:
        header_split = header.split(": ")
        return_headers[header_split[0]] = [header_split[1]]
    return return_headers


def get_mili_seconds(time_str):
    hh, mm, ss = time_str.split(":")
    ss, ms = ss.split(".")
    return (float(hh) * 3600 + int(mm) * 60 + int(ss)) * 1000 + int(
        float("." + ms) * 1000
    )


def bulk_post(data):
    ndjson_data = ndjson.dumps(data)
    API_URL = os.getenv(
        "FIRETAIL_URL",
        "https://api.logging.eu-west-1.sandbox.firetail.app/logs/azure/apim/bulk",
    )
    response = session.post(
        url=API_URL,
        data=ndjson_data,
        headers={
            "Content-Type": "application/x-ndjson",
            "x-ft-app-key": os.getenv("FIRETAIL_APP_TOKEN"),
        },
    )
    if response.status_code != 201:
        raise Exception("failed to write data to firetail, " + response.text)


def rewrite_data(data):
    executionTime = get_mili_seconds(data.get("executionTime", "00:00:00.00"))
    return_data = {
        "version": "1.0.0-alpha",
        "metadata": {
            "apiId": data.get("apiId"),
            "gatewayId": data.get("serviceId"),
            "requestId": data.get("messageId"),
        },
        "executionTime": int(executionTime),
        "dateCreated": convert_datetime_to_seconds(data.get("requestContextTimestamp")),
        "request": {
            "body": data.get("requestBody"),
            "httpProtocol": "HTTP/1.1",  # not available in policy
            "uri": data.get("uri"),
            "resource": data.get("urlTemplate", "/"),
            "method": data.get("requestMethod").upper(),
            "ip": data.get("ip"),
        },
        "response": {
            "statusCode": data.get("statusCode"),
            "body": data.get("responseBody"),
        },
    }

    return_data["response"]["headers"] = convert_headers_to_dict(
        data.get("responseHeaders", "")
    )
    return_data["request"]["headers"] = convert_headers_to_dict(
        data.get("requestHeaders", "")
    )
    return return_data


@app.function_name(name="fteventhublogsTrigger")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name=os.getenv("EVENT_HUB_NAME", "test2"),
    connection="eventhub_connection_str",
)
def test_function(event: func.EventHubEvent):
    event = json.loads(event.get_body().decode("utf-8"))
    logging.info(
        "Python EventHub trigger processed an event: %s",
    )
    data = [rewrite_data(event)]
    bulk_post(data)
