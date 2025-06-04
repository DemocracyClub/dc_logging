import boto3


def handler(event, context):
    """
    AWS Lambda function that retrieves specific parameter store variables
    and returns them as a dictionary of name: value
    """
    ssm = boto3.client("ssm")
    parameter_names = event.get("parameter_names", [])

    if not parameter_names:
        raise Exception("No parameter names provided")

    parameters = {}

    for param_name in parameter_names:
        try:
            response = ssm.get_parameter(
                Name=param_name,
            )
            parameters[param_name] = response["Parameter"]["Value"]

        except ssm.exceptions.ParameterNotFound:
            print(f"Parameter not found: {param_name}")
            raise
        except Exception as e:
            print(f"Error retrieving parameter {param_name}: {str(e)}")
            raise

    return parameters
