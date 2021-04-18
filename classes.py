class JobListing:
    def __init__(self, company: str, link: str, description:str = None, location: str = None):
        self.company = company
        self.link = link
        self.description = description
        self.location = location