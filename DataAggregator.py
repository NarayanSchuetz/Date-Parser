#Defines a day aggregator object
import datetime

"""Node and Leaf classes helping to build a date tree structure, could be expanded easily"""


class MainContainer(object):
    """
    Container class holding all  derived instances
    """
    def __init__(self, date=None, _type="", sensor="", _id="DummyID", name="container"):
        self.id = _id
        self.type = _type
        self.sensor = sensor
        self.children = []
        self.date = self.correct_date(date)
        self.value = None
        self.min_value = None
        self.max_value = None
        self.actual_child = 0
        self.name = name

    def __str__(self):
        return "Date: %s, Type: %s, Sensor: %s" % (str(self.date), self.type, self.sensor)

    def __eq__(self, other):
        return True

    def __add__(self, other):
        return self.value + other.value

    def __iadd__(self, other):
        self.value += other.value
        return self

    #Recursively calculates the value
    def aggregate(self):
        value = 0
        for child in self.children:
            value += child.aggregate()
        result = float(value) / len(self.children)
        self.value = result
        return result

    def correct_date(self, date):
        return date

    def get_value(self):
        if self.value is not None:
            return self.value
        else:
            self.aggregate()
            return self.value

    def get_min_value(self):
        if self.min_value is not None:
            return self.min_value
        else:
            instance_min = float("inf")
            for child in self.children:
                local_min = child.get_min_value()
                if local_min < instance_min:
                    instance_min = local_min
            self.min_value = instance_min
            return instance_min

    def get_max_value(self):
        if self.max_value is not None:
            return self.max_value
        else:
            instance_max = 0
            for child in self.children:
                local_max = child.get_max_value()
                if local_max > instance_max:
                    instance_max = local_max
            self.max_value = instance_max
            return instance_max

    def add_child(self, timestamp):
        self.children.append(Day(date=timestamp))
        self.actual_child += 1


class Day(MainContainer):
    """Day class, representing a day instance"""
    def __init__(self, date):
        super().__init__(date=date)
        self.name = "day"

    # Magic methods, allowing comparison of datetime objects in the resolution of the Container items
    def __eq__(self, other):
        other_date = self.correct_date(other)
        if self.date == other_date:
            return True
        else:
            return False

    def __lt__(self, other):
        other_date = self.correct_date(other)
        if self.date < other_date:
            return True
        else:
            return False

    def __gt__(self, other):
        other_date = self.correct_date(other)
        if self.date > other_date:
            return True
        else:
            return False

    def correct_date(self, date):
        date = date.replace(hour=0, minute=0)
        return date

    def add_child(self, timestamp):
        self.children.append(Hour(date=timestamp))
        self.actual_child += 1


class Hour(Day):

    def __init__(self, date):
        super().__init__(date=date)
        self.name = "hour"

    def correct_date(self, date):
        date = date.replace(minute=0)
        return date

    def add_child(self, timestamp):
        self.children.append(Minute(date=timestamp))
        self.actual_child += 1


class Minute(Hour):
    """Leaf node class, overrides several methods as a result of its leaf node function"""
    def __init__(self, date):
        super().__init__(date=date)
        self.name = "minute"

    def aggregate(self):
        try:
            value = 0
            for child in self.children:
                value += child
            result = float(value) / len(self.children)
            self.value = result
            return result
        except (ValueError, TypeError):
            raise AssertionError('Extracted Values are not numeric, cannot calculate mean')

    def get_min_value(self):
        try:
            instance_min = min(self.children)
            self.min_value = instance_min
            return instance_min
        except (ValueError, TypeError):
            raise AssertionError('Extracted Values are not numeric, cannot calculate minimum')

    def get_max_value(self):
        try:
            instance_max = max(self.children)
            self.max_value = instance_max
            return instance_max
        except (ValueError, TypeError):
            raise AssertionError('Extracted Values are not numeric, cannot calculate maximum')

    def get_raw_values(self):
        return self.children

    def correct_date(self, date):
        return date

    def add_child(self, value):
        for i in value:
            self.children.append(i)