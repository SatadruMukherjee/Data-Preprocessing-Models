import json
from aws_lambda_powertools.event_handler.exceptions import NotFoundError
from aws_lambda_powertools.event_handler import (
    APIGatewayRestResolver,
    Response,
    content_types,
)
app = APIGatewayRestResolver()

@app.not_found
def handle_not_found_errors(exc: NotFoundError) -> Response:
    return Response(status_code=418, content_type=content_types.TEXT_PLAIN, body="No such resource path found")
    
@app.get("/v1")
def v1_call():
    print("Inside v1")
    return {"path":1}
   
@app.get("/v2")
def v2_call():
    print("Inside v2")
    return {"path":2}
    
@app.get("/v4/<animal_name>")
def v4_call(animal_name: str):
    print("Inside v4")
    return {"value":animal_name}
    
    
@app.post("/v3")
def v3_call():
    print("Inside v3 post endpoint")
    todo_data: dict = app.current_event.json_body
    return f"I love my {todo_data["country"]}"
    
def lambda_handler(event, context):
    # TODO implement
    print("Input Event: ",event)
    return app.resolve(event, context)
