from fastapi import FastAPI,Path,HTTPException,Query
import json
app = FastAPI()

def load_data():
    with open('patient.json','r') as f:
        data = json.load(f)

    return data

@app.get("/")
def hello():
    return {'message':'patient Management System API'}
@app.get('/about')
def about():
    return {'message':'A fully function API to manage your patient'}

@app.get('/view')
def view():
    data = load_data()
    return data
@app.get('/patient/{patient_id}')
def view_patient(patient_id:str = Path(...,description = 'id of the patient in db',example = 'P001')):
    data = load_data()
    if patient_id in data:
        return data[patient_id]
    
    raise HTTPException(status_code=404,detail='patient not found')

@app.get('/sort')
def sort_patients(sort_by:str = Query(..., description='sort on the of height,weight,bmi')
,order:str = Query('asc',description='sort in asc or dec order')):
    valid_fields = ['height','weight','bmi']
    if sort_by not in valid_fields:
        raise HTTPException(status_code=400,detail=f'invalid field select from {valid_fields}')
    if order not in ['asc','desc']:
        raise HTTPException(status_code=400,detail=f'invalid field select between asc and desc')
    data = load_data()
    sort_order = True if order == 'desc' else False
    sorted_data = sorted(data.values(),key=lambda x: x.get(sort_by,0),reverse=sort_order)
    return sorted_data