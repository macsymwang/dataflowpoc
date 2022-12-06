import json

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        runFlex()
        return f'Hello World!'

def runFlex():
    from googleapiclient.discovery import build
    project = 'cio-mediation-springdf-lab-3f'
    job = 'unique-job-name-flex'
    template = 'gs://cio-mediation-springdf-lab-3f-dataflow/template/cio-mediation-datahub-load.json'



    parameters = {
        "inputSubscription": "projects/cio-mediation-springdf-lab-3f/subscriptions/gcsTopic.input",
        "outputTable": "cio-mediation-springdf-lab-3f:sample_ds.order_details",
        "tableSchemaPath": "gs://loony-learn/config_files/schema.json"
    }

    environment = {
        'network': 'default',
        'subnetwork': 'regions/northamerica-northeast1/subnetworks/default',
        'ipConfiguration': 'WORKER_IP_PRIVATE',
        'workerRegion': 'northamerica-northeast1',
        "enableStreamingEngine": True,
    }

    launchFlexTemplateParameter = {
        'jobName': job,
        'parameters': parameters,
        'environment': environment,
        'containerSpecGcsPath': template
    }


    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().locations().flexTemplates().launch(
        projectId=project,
        location='northamerica-northeast1',
        body={
            'launchParameter': launchFlexTemplateParameter
        }
    )

    response = request.execute()
    print(response)
    
def runWordCount():
    from googleapiclient.discovery import build
    project = 'cio-mediation-springdf-lab-3f'
    job = 'unique-job-name'
    template = 'gs://dataflow-templates/latest/Word_Count'
    parameters = {
        'inputFile': 'gs://dataflow-samples/shakespeare/kinglear.txt',
        'output': 'gs://loony-learn/wordcount/outputs',
    }

    environment = {
        'network': 'default',
        'subnetwork': 'regions/northamerica-northeast1/subnetworks/default',
        'ipConfiguration': 'WORKER_IP_PRIVATE',
        'workerRegion': 'northamerica-northeast1'
    }

    dataflow = build('dataflow', 'v1b3')
    request = dataflow.projects().templates().launch(
        projectId=project,
        gcsPath=template,
        body={
            'jobName': job,
            'parameters': parameters,
            "environment": environment
        }
    )

    response = request.execute()
    print(response)