import classes
from classes import JobListing
import confuse
from confuse import Configuration
import scrapers
from scrapers import LinkedInScraper, GithubScraper
from typing import List

def main():   
    #Load configuration from config.yaml
    config = confuse.Configuration('InternshipScraper', __name__)
    config.set_file('config.yaml')
    
    #The LinkedIn scrape takes a long time if you put in a lot of locations/search terms
    linkedInScrapeEnabled = config["linkedIn"]["enabled"].get()
    githubScrapeEnabled = config["github"]["enabled"].get()

    if(githubScrapeEnabled):
        jobs = scrapeGithub(config)
        if jobs is not None:
            outputToFile(jobs, "output/github-scrape.txt")

    if(linkedInScrapeEnabled):
        jobs = scrapeLinkedIn(config)
        if jobs is not None:
            outputToFile(jobs, "output/linkedIn-scrape.txt")

def outputToFile(jobs: List[JobListing], fileName: str) -> None:
    newPositions = 0

    readFile = open(fileName, "r")
    existingLines = readFile.readlines()
    readFile.close()

    for job in jobs:
        jobStr = job.company + " | " + job.link + "\n"
        if jobStr not in existingLines:
            existingLines.append(jobStr)
            newPositions += 1

    existingLines.sort()
    writeNewLines(jobs, fileName, existingLines)
    print("Found {} new positions for {}".format(str(newPositions), fileName))


def writeNewLines(jobs: List[JobListing], fileName: str, existingLines: List[str]) -> None:
    writeFile = open(fileName, "w")
    for line in existingLines:
        writeFile.write(line)
    writeFile.close()
    

def scrapeLinkedIn(config: Configuration) -> List[JobListing]:
    queries = config["linkedIn"]["queries"].get()
    locations = config["linkedIn"]["locationsToQuery"].get()
    titles = config["linkedIn"]["desiredJobTitles"].get()
    blacklist = config["linkedIn"]["description"]["blacklistSubstrings"].get()
    required = config["linkedIn"]["description"]["requiredSubstrings"].get()
    scraper = LinkedInScraper(queries, locations, titles, blacklist, required)
    jobs = scraper.scrapeJobs()
    return jobs

def scrapeGithub(config: Configuration) -> List[JobListing]:
    repoURL = config["github"]["repoURL"].get()
    scraper = GithubScraper(repoURL)
    jobs = scraper.scrapeJobs()
    return jobs

if __name__ == "__main__":
    main()