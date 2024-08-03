# Create and Deploy a ML model using G


*In this post. I will explain how to expose an API from a trained model, use best CI/CD practices (Github Actions) and IaC (Terraform) to automate infrastructure creation.*


## Prerrequisites

- Docker Desktop 
- Git 
- Github Account


##  num 1: still not done




## num2: Run using FastAPI, for now - locally

 1. Run `docker build -t mlops-fastapi .` in the root of the project to build the image of the api.
 2. Run  `docker run -d --name ml -p 80:8080 mlops-fastapi` to create the container using ml-api image built.
 3. Open [localhost](http://localhost/docs) to test the project.
 4. On /predict/ post endpoint, you can use this body as an example:
 
   ```  
  {
                "TotalCharges":float,
                'Month-to-month':int,
                'One year':int
                'Two year':int
                'PhoneService': int,
                'tenure': int
            }
```
 5. You should expect a response 200 with a `"prediction": _____` which means __________-_.

 

 
