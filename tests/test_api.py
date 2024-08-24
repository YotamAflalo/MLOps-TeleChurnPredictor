from fastapi.testclient import TestClient
from src.models.api.app import app

client = TestClient(app)

def test_info():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"instructions": """Please provide a JSON file in the following format: 
            {
                "TotalCharges": str or int,
                "Contract": str,
                "PhoneService": str,
                "tenure": int
            }"""}

def test_single_prediction0():
    response = client.post(
        "/predict/",
        json={"TotalCharges":"1889.5","Contract":"One year", "PhoneService":"Yes","tenure": 34},
    )
    assert response.status_code == 200
    assert response.json() == {  "prediction": 0}

# def test_single_prediction1():
#     pass

def test_single_prediction0_more_data():
    response = client.post(
        "/predict/",
        json={"customer_id":"52232","TotalCharges":"1889.5","Contract":"One year", "PhoneService":"Yes","tenure": 34,"age":99},
    )
    assert response.status_code == 200
    assert response.json() == {"prediction": 0}

# def test_single_prediction0_less_data_fixable():
#     response = client.post(
#         "/predict/",
#         json={"TotalCharges":"1889.5","Contract":"One year","tenure": 34},
#     )
#     assert response.status_code == 200
#     assert response.json() == {"prediction": 0}


def test_single_prediction_wrong_data_types_fixable():
    response = client.post(
        "/predict/",
        json={"TotalCharges":1889.5,"Contract":"One year", "PhoneService":"Yes","tenure": 34},
    )
    assert response.status_code == 200
    assert response.json() == {"prediction": 0}


# def test_single_prediction_wrong_data_types_unfixable():
#     pass
