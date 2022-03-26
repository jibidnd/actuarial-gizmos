from rating import rating_table

class RatingPlan:
    """This class handles the initialization of a rating plan."""

    def __init__(self, name: str) -> None:
        
        self.name = name
        self.rating_tables = {}
    
    def add_rating_table(self, name: str, rating_table: rating_table):
        self.rating_table[name]= rating_table
