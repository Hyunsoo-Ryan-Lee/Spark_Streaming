from faker import Faker
import shortuuid
from datetime import datetime

fake = Faker()
fake.profile()

def create_fakeuser() -> dict:
    
    fake = Faker()
    fake_profile = fake.profile()
    key_list = ["name", "ssn", "job", "residence", "blood_group", "sex", "birthdate"]

    fake_dict = dict()

    for key in key_list:
        fake_dict[key] = fake_profile[key]
        
    fake_dict["uuid"] = shortuuid.uuid()

    fake_dict['birthdate'] = fake_dict['birthdate'].strftime("%Y%m%d")
    fake_dict['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return fake_dict

if __name__ == "__main__":
    user = create_fakeuser()
    print(user)