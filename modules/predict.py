import os
import json
import dill
import pandas as pd
from datetime import datetime

path = os.environ.get('PROJECT_PATH', '.')

def get_model():
    model_path = f'{path}/data/models/cars_pipe_{datetime.now().strftime("%Y%m%d%H")}.pkl'
    with open(model_path, "rb") as f:
        return dill.load(f)
    
def get_test_data():
    folder_path = f"{path}/data/test"

    jsons = list()

    for file_name in os.listdir(folder_path):

        file_path = os.path.join(folder_path, file_name)

        if os.path.isfile(file_path):  

            with open(file_path, "r", encoding="utf-8") as f:
                jsons.append(json.load(f))

    return jsons
            
def save_pred_to_csv(preds):
    df = pd.DataFrame.from_dict({'predictions': preds})

    csv_path = f'/opt/airflow/data/predictions/prediction.csv'

    with open(csv_path, "w") as f:
        df.to_csv(csv_path)
    

def predict():
    model = get_model()

    cars = get_test_data()
    
    preds = list()

    for car in cars:
        df = pd.DataFrame.from_dict([car])
        pred = model.predict(df)
        preds.append(pred[0])

    save_pred_to_csv(preds)


if __name__ == '__main__':
    predict()