# -*- coding: utf-8 -*-



class Loc(object):

    def __init__(self):
        self._loc = {
            "country": None,
            "province": None,
            "city": None,
            "district": None,
            "street": None,
            "address": None,
            "point": {}
        }

    def setCountry(self, country):
        self._loc["country"] = country if country else None

    def setProvince(self, province):
        self._loc["province"] = province if province else None

    def setCity(self, city):
        self._loc["city"] = city if city else None

    # def setCityCode(self, city_code):
    #     self._loc["city_code"] = city_code

    def setDistrict(self, district):
        self._loc["district"] = district if district else None

    def setStreet(self, street):
        self._loc["street"] = street if street else None

    def setAddress(self, address):
        self._loc["address"] = address if address else None

    def setPoint(self, point):
        self._loc["point"] = point if point else {}

    def builder(self):
        return self._loc
