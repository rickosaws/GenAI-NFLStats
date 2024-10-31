import os
import boto3
from boto3.dynamodb.conditions import Key

def transform_team_name(team_name):
    """Transform team name to lowercase and remove spaces"""
    return str(team_name).lower().replace(" ", "").strip()

def transform_coaching_position(position):
    """Transform coaching position by removing 'coach' and converting to lowercase"""
    position = str(position).lower().strip()
    return position.replace("coach", "").strip()

def get_parameter_value(parameters, param_name):
    """Helper function to get parameter value by name"""
    for param in parameters:
        if param.get('name').lower() == param_name.lower():
            return param.get('value')
    return None

def getCoachData(teamName, coachingPosition, year):
    # Transform the inputs
    teamName = transform_team_name(teamName)
    coachingPosition = transform_coaching_position(coachingPosition)
    year = str(year).strip()
    
    # Concatenate the teamName and coachingPosition using a hash '#'
    pk = f"{teamName}#{coachingPosition}"
    
    try:
        # Create a DynamoDB client
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ["CoachData_table"])
        
        print(f"Querying DynamoDB with pk: {pk}, sk: {year}")  # Debug log
        
        # Query the CoachData table with the pk and sk
        response = table.get_item(
            Key={
                'pk': pk,
                'sk': year
            }
        )
        
        # Return the coach data if found, otherwise return None

        return response.get('Item')
    
    except Exception as e:
        print(f"Error querying DynamoDB: {str(e)}")
        print(f"Query parameters - pk: {pk}, sk: {year}")
        return None

def lambda_handler(event, context):
    try:
        agent = event['agent']
        actionGroup = event['actionGroup']
        function = event['function']
        parameters = event.get('parameters', [])

        # Extract parameters using the helper function
        year = get_parameter_value(parameters, 'year')
        position = get_parameter_value(parameters, 'position')
        team = get_parameter_value(parameters, 'TeamName')

        # Validate parameters
        if not all([year, position, team]):
            raise ValueError("Missing required parameters. Need team, position, and year.")

        # Call the getCoachData function
        coachData = getCoachData(team, position, year)
        print(f"The response from DynamoDB is {coachData}")
        
        # Store original values for response formatting
        original_team = team
        original_position = position
        
        if coachData:
            # Coach data found, format the response
            responseBody = {
                "TEXT": {
                    "body": f"The {original_team} {original_position} coach in the {year} Season was {coachData.get('Coach', 'Unknown')}"
                }
            }
        else:
            # Coach data not found, provide an appropriate response
            responseBody = {
                "TEXT": {
                    "body": f"No coach data found for {original_team} {original_position} in {year}"
                }
            }

        action_response = {
            'actionGroup': actionGroup,
            'function': function,
            'functionResponse': {
                'responseBody': responseBody
            }
        }

        function_response = {'response': action_response, 'messageVersion': event['messageVersion']}
        print("Response: {}".format(function_response))

        return function_response

    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        # Return a formatted error response
        error_response = {
            'response': {
                'actionGroup': event.get('actionGroup', ''),
                'function': event.get('function', ''),
                'functionResponse': {
                    'responseBody': {
                        "TEXT": {
                            "body": "Sorry, there was an error processing your request."
                        }
                    }
                }
            },
            'messageVersion': event.get('messageVersion', '1.0')
        }
        return error_response
