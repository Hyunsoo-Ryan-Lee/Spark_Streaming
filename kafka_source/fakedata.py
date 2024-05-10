from faker import Faker
import shortuuid

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

    return fake_dict

if __name__ == "__main__":
    user = create_fakeuser()
    print(user)