# Databricks notebook source

from typing import Any, Dict, Type, TypeVar

class CustomDecoder():
    T = TypeVar('T')

    def __init__(self, source_object: Any, target_class: Type[T]) -> None:
        """
        Initialize the decoder with a source object and the target class.
        
        Args:
            source_object: The object to be decoded.
            target_class: The class to which the object should be converted.
        """
        self.source_object = source_object
        self.target_class = target_class

    def decode(self) -> T:
        """
        Decode the source object to an instance of the target class.
        
        Returns:
            An instance of the target class with attributes populated from the source object.
        """
        decoded_object = {}

        source_attrs = self.source_object.__dict__
        target_attrs = self.target_class.__init__.__annotations__

        # assuming that classes have the same number of attributes
        for source_value, (target_attr, target_type) in zip(source_attrs.values(), target_attrs.items()):
            decoded_attr = CustomDecoder.any_to_any(source_value, target_type)
            decoded_object[target_attr] = decoded_attr

        return self.target_class(**decoded_object)

    @staticmethod
    def any_to_any(source_value: Any, target_type: Type) -> Any:
        """
        Convert a source value to the target type.
        
        Args:
            source_value: The value to be converted.
            target_type: The type to which the value should be converted.
        
        Returns:
            The converted value.
        """        
        if isinstance(source_value, target_type):
            decoded_attr = source_value
        elif isinstance(source_value, str):
            decoded_attr = CustomDecoder.str_to_any(source_value, target_type)
        elif target_type == str:
            decoded_attr = CustomDecoder.any_to_str(source_value)            
        ### add more conversions
        else:
            raise NotImplementedError
        return decoded_attr
    
    @staticmethod
    def str_to_any(value: str, target_type: Type) -> Any:
        """
        Convert a string value to the target type.
        
        Args:
            value: The string value to be converted.
            target_type: The type to which the value should be converted.
        
        Returns:
            The converted value.
        """
        if target_type == int:
            converted_value = int(value)
        elif target_type == float:
            converted_value = float(value)
        elif target_type == bool:
            converted_value = bool(value)
        elif target_type == list:
            converted_value = value.split(delimiter=",")
        else: 
            converted_value = value
        return converted_value
    
    @staticmethod
    def any_to_str(value: Any) -> str:
        """
        Convert any value to a string.
        
        Args:
            value: The value to be converted.
        
        Returns:
            The converted string.
        """

        if isinstance(value, list):
            converted_value = ",".join(str(item) for item in value)
        else:
            converted_value = str(value)
        return converted_value

# COMMAND ----------

class myHospitalPatient:
    def __init__(self, name: str, surname: str, age: int, salary: str):
        self.name = name
        self.surname = surname
        self.age = age
        self.salary = salary
    
    def __repr__(self):
        return f"myHospitalPatient(name={self.name}, surname={self.surname}, age={self.age}, salary={self.salary})"
    
class InternalPatient:
    def __init__(self, given_name: str, lastname: str, age: int, my_salary: float):
        self.given_name = given_name
        self.lastname = lastname
        self.age = age
        self.my_salary = my_salary

    def __repr__(self):
        return f"InternalPatient(given_name={self.given_name}, lastname={self.lastname}, age={self.age}, my_salary={self.my_salary})"  

# COMMAND ----------

internal_patient_obj = InternalPatient(given_name=["FirstName","SecondName"], lastname="LastName", age=23, my_salary=1200.8)
d = CustomDecoder(internal_patient_obj, myHospitalPatient)
d.decode()

# COMMAND ----------

myHospitalPatient_obj = myHospitalPatient(name="Name", surname="Surname", age=23, salary="1300.7")
d = CustomDecoder(myHospitalPatient_obj, InternalPatient)
print(d.decode())
print(type(d.decode().my_salary))
