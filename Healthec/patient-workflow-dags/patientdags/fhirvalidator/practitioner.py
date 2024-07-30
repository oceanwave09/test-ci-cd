from typing import Union

from patientdags.fhirvalidator.base import Base


class Prcatitioner(Base):
    COMMA_SYMBOL = ","

    def __init__(self, resource: Union[dict, str]) -> None:
        super().__init__(resource)

    def update_name(self):
        if len(self.resource.get("name", [])) == 0:
            return
        names = self.resource.get("name", [])
        for name in names:
            if name.get("family") and self.COMMA_SYMBOL in name.get("family") and not name.get("given"):
                name["given"] = [name.get("family").split(",")[0]]
                name["family"] = name.get("family").split(",")[1]

    def update_birth_date(self):
        if not self.resource.get("birthDate"):
            return
        birth_date = self._format_date(self.resource.get("birthDate"))
        if birth_date:
            self.resource.update({"birthDate": birth_date})

    def validate_email(self):
        telecoms = self.resource.get("telecom", [])
        for telecom in telecoms:
            if telecom.get("system") == "email":
                return
        raise ValueError("Email is required for practitioner resource")

    def validate_name(self):
        if len(self.resource.get("name", [])) == 0:
            raise ValueError("Firstname and Lastname are required for practitioner resource")
        names = self.resource.get("name", [])
        for name in names:
            if len(name.get("given", [])) == 0:
                raise ValueError("Firstname and Lastname are required for practitioner resource")
            if not name.get("family", ""):
                raise ValueError("Firstname and Lastname are required for practitioner resource")

    def validate(self):
        self.validate_resource_identifier()
        # self.validate_email()
        self.validate_name()

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_name()
        self.update_birth_date()
