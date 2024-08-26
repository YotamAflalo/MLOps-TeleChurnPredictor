# Create and Deploy a ML model using G


*In this post. I will explain how to expose an API from a trained model, use best CI/CD practices (Github Actions) and IaC (Terraform) to automate infrastructure creation.*


## Prerrequisites

- Docker Desktop 
- Git 
- Github Account


##  batch 

the architecture of the project is as follows:
### 1. Data Collection - can be either from a csv in data/raw or from a database (only unpredicted data, based on the timestamp of the last prediction)
### 2. Data Preprocessing - the data is cleaned and transformed to be used in the model (beam_preprocessing.py)
### 3. output - the data is saved in the data/batch_results folder.


## Run the API with FastAPI (for now - locally)
 1. run `cd docker`
 2. Run `docker compose -f docker-compose.yaml up --build` in the docker folder in the project to build the image of the api.
 3. You may open [\[localhost\](http://localhost/docs)](http://localhost:8005/docs) to test the API.
 4. On /predict/ post endpoint, you can use this body as an example:
 
   ```  
  {
                "TotalCharges":"1889.5",
                "Contract":"One year",
                "PhoneService":"Yes",
                "tenure": 34
            }
```
 5. You should expect a response 200 with a `"prediction": 0` which means the cleint will not leave soon.
 
## monitor with grafana
1. enter [text](http://localhost:3000/)
2. go to "dashboards"
3. enjoy!
![alt text](dashboard.png)



 
