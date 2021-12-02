import motor.motor_asyncio
from bson.objectid import ObjectId
from decouple import config
from outfit import Outfit, Logger, ConsulCon, VaultCon, merge_dict


__confit_info__ = 'configs/config.yaml'

# load config via python-outfit
Outfit.setup(__confit_info__)

vault = VaultCon().get_secret_kv()

consul = ConsulCon().get_kv()

# merge dict from vault and consul
config_set = merge_dict(consul, vault)



# MONGO_DETAILS = config('MONGO_DETAILS') 
# read environment variable.
uri = "mongodb://%s:%s@%s:%d/%s" % (
    config_set['mongodb']['username'], 
    config_set['mongodb']['password'], 
    config_set['mongodb']['host'], config_set['mongodb']['port'],
    config_set['mongodb']['database'])

Logger.info(uri)
print(uri)
client = motor.motor_asyncio.AsyncIOMotorClient(uri)
'''
client = motor.motor_asyncio.AsyncIOMotorClient(
            config_set['mongodb']['host'], 
            config_set['mongodb']['port'], 
            username = config_set['mongodb']['username'], 
            password = config_set['mongodb']['password'], 
            authSource = config_set['mongodb']['database'], 
        )
'''

database = client[config_set['mongodb']['database']]
print(config_set['mongodb']['database'])
student_collection = database.get_collection("students_collection")


# helpers


def student_helper(student) -> dict:
    return {
        "id": str(student["_id"]),
        "fullname": student["fullname"],
        "email": student["email"],
        "course_of_study": student["course_of_study"],
        "year": student["year"],
        "GPA": student["gpa"],
    }


# crud operations

# Retrieve all students present in the database
async def retrieve_students():
    students = []
    async for student in student_collection.find():
        students.append(student_helper(student))
    return students


# Add a new student into to the database
async def add_student(student_data: dict) -> dict:
    student = await student_collection.insert_one(student_data)
    new_student = await student_collection.find_one({"_id": student.inserted_id})
    return student_helper(new_student)


# Retrieve a student with a matching ID
async def retrieve_student(id: str) -> dict:
    student = await student_collection.find_one({"_id": ObjectId(id)})
    if student:
        return student_helper(student)


# Update a student with a matching ID
async def update_student(id: str, data: dict):
    # Return false if an empty request body is sent.
    if len(data) < 1:
        return False
    student = await student_collection.find_one({"_id": ObjectId(id)})
    if student:
        updated_student = await student_collection.update_one(
            {"_id": ObjectId(id)}, {"$set": data}
        )
        if updated_student:
            return True
        return False


# Delete a student from the database
async def delete_student(id: str):
    student = await student_collection.find_one({"_id": ObjectId(id)})
    if student:
        await student_collection.delete_one({"_id": ObjectId(id)})
        return True
