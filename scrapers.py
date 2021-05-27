from bs4 import BeautifulSoup
from classes import JobListing
from typing import List
import aiohttp
import asyncio
import time
from aiohttp import ClientSession
import requests

class LinkedInScraper:
    def __init__(self, searchTerms: List[str], locations: List[str], jobTitles: List[str] = None, blacklistJobTitles: List[str] = None,
                 blacklistSubstrings: List[str] = None, requiredSubstrings: List[str] = None, timespan: str = None) -> None:
        self.searchTerms = searchTerms
        self.locations = locations
        self.jobTitles = jobTitles
        self.blacklistSubstrings = blacklistSubstrings
        self.requiredSubstrings = requiredSubstrings
        self.blacklistJobTitles = blacklistJobTitles

        validTimespanToSecondsDict = {
            "day": "r86400",
            "week": "r604800",
            "month": "r2592000"
        }
        self.timespan = validTimespanToSecondsDict[timespan] if timespan in validTimespanToSecondsDict else None

    async def fetchHTML(self, url: str, session: ClientSession) -> str:
        resp = await session.request(method="GET", url=url)
        html = await resp.text()
        if(len(html) == 0):
            print("Hit rate limit for LinkedIn requests")
        return html
        
    async def gatherTopLevelSearchForLocations(self) -> List[str]:
        htmlList = []
        semaphore = asyncio.Semaphore(2)
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                for location in self.locations:
                    requestStr = "https://www.linkedin.com/jobs/search?keywords={}&location={}".format(self.searchTerms[0], location)
                    html = await self.fetchHTML(requestStr, session)
                    htmlList.append(html)
        return htmlList

    async def gatherCityLevelSearch(self, requests: List[str]) -> List[str]:
        htmlList = []
        semaphore = asyncio.Semaphore(2)
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                for request in requests:
                    html = await self.fetchHTML(request, session)
                    htmlList.append(html) 
        return htmlList

    async def gatherHTMLForJobPostings(self, jobLinks: List[str]) -> List[str]:
        htmlList = []
        async with aiohttp.ClientSession() as session:
            for link in jobLinks:
                html = await self.fetchHTML(link, session)
                htmlList.append(html)
        return htmlList

    def getTopLevelSearch(self) -> List[str]:
        #doing this instead of asyncio.run because of this issue: https://github.com/aio-libs/aiohttp/issues/4324
        return asyncio.get_event_loop().run_until_complete(self.gatherTopLevelSearchForLocations())

    def getCityLevelSearch(self, requests: List[str]) -> List[str]:
        #doing this instead of asyncio.run because of this issue: https://github.com/aio-libs/aiohttp/issues/4324
        return asyncio.get_event_loop().run_until_complete(self.gatherCityLevelSearch(requests))

    def getJobPostingsHTML(self, jobLinks: List[str]) -> List[str]:
        return asyncio.get_event_loop().run_until_complete(self.gatherHTMLForJobPostings(jobLinks))

    def parseJobLinksFromTopLevelSearches(self, searchList: List[str]) -> List[str]:
        seenJobHashes = set()
        jobLinks = []
        
        for html in searchList:
            soup = BeautifulSoup(html, features="html.parser")
            for jobCard in soup.select("ul.jobs-search__results-list"):
                companyName = None
                jobTitle = None

                jobTitleTag = jobCard.find("span", class_=["screen-reader-text"])
                if jobTitleTag is not None:
                    jobTitle = str(jobTitleTag.string).strip()
                    if not self.filterJobTitles(jobTitle):
                        continue

                companyNameTag = jobCard.find("a", class_=["job-search-card__subtitle"])
                if companyNameTag is not None:
                    companyName = str(companyNameTag.string).strip()
                
                if companyName is not None and jobTitle is not None:
                    hashString = companyName + "_" + jobTitle
                    if hashString not in seenJobHashes:
                        seenJobHashes.add(hashString)
                        jobPostDetails = jobCard.find("a", class_=["base-card__full-link"])
                        jobPostLink = jobPostDetails.get("href")
                        jobLinks.append(jobPostLink)

        return list(jobLinks)

    def getUniqueCityCodes(self, searchHTML: List[str]) -> List[str]:
        cityCodes = set()
        for html in searchHTML:
            soup = BeautifulSoup(html, features="html.parser")
            locationOptions = soup.find_all("input", {"name":"f_PP"})
            for option in locationOptions:
                cityCode = (option.get("value"))
                cityCodes.add(cityCode)
        return cityCodes

    def filterBlacklist(self, description: str) -> bool:
        #if any blacklist words are in the description, return false
        lowerDesc = description.lower()
        blacklist = self.blacklistSubstrings
        if blacklist is not None and len(blacklist) > 0:
            for word in blacklist:
                if word.lower() in lowerDesc:
                    return False
        return True    

    def filterRequired(self, description: str) -> bool:
        #if any required words are not in the description, return false
        lowerDesc = description.lower()
        required = self.requiredSubstrings
        if required is not None and len(required) > 0:
            for word in required:
                if word.lower() not in lowerDesc:
                    return False
        return True    

    def filterJobTitles(self, jobTitle:str) -> bool:
        #if any one of our required job titles in the job title and none of the blacklist keywords, return true
        lowerTitle = jobTitle.lower()
        blacklistJobTitles = self.blacklistJobTitles
        if blacklistJobTitles is not None and len(blacklistJobTitles) > 0:
            for blacklistTitle in blacklistJobTitles:
                if blacklistTitle.lower() in lowerTitle:
                    return False
        titles = self.jobTitles
        if titles is not None and len(titles) > 0:
            for title in titles:
                if title.lower() in lowerTitle:
                    return True
        return False

    def parseJobInformationFromJobLinks(self, jobPostingsList: List[str]) -> List[JobListing]:
        print("Parsing jobs from job-specific links...")
        seenLinks = set()
        jobInformation = []
        numSkipped = 0
        numFailed = 0

        for html in jobPostingsList:
            soup = BeautifulSoup(html, features="html.parser")
            companyTag = soup.find('a', class_=['sub-nav-cta__optional-url'])
            if(companyTag is None):
                numFailed += 1 
                continue

            company = companyTag.get("title")
            descriptionTag = soup.find('div', class_=['show-more-less-html__markup'])
            description = " ".join(str(line) for line in descriptionTag.strings) 

            if not self.filterBlacklist(description) or not self.filterRequired(description):
                numFailed += 1 
                continue

            allLinks = [link.get('href') for link in soup.find_all('a')]
            jobLink = next((link for link in allLinks if "externalApply" in link), None)

            if jobLink is None:
                numFailed += 1 
                continue
            
            #Everything from &refId onwards in the linkedIn URL can vary for the same posting - so remove it
            refIdIndex = jobLink.find("&refId")
            if refIdIndex != -1:
                jobLink = jobLink[:refIdIndex+1]

            if jobLink not in seenLinks:
                seenLinks.add(company)
                newJobListing = JobListing(company, jobLink, description=description)
                jobInformation.append(newJobListing)
            else:
                numSkipped += 1 

        print("Skipped {} postings because they were duplicates and {} because they failed to meet criteria".format(numSkipped, numFailed))
        return jobInformation
        
    def getCityLevelSearchInBatches(self, cityCodes: List[str]) -> List[str]:
        citySearchHTMLList = []

        approxTimeDelay = len(self.searchTerms) / 4.0

        timeSpanArg = "&f_TPR=" + self.timespan if self.timespan is not None else ""

        for cityCode in cityCodes:
            requests = []
            for searchTerm in self.searchTerms:
                requestStr = "https://www.linkedin.com/jobs/search?keywords={}&f_PP={}{}".format(searchTerm, cityCode, timeSpanArg)
                requests.append(requestStr)
            cityHTML = self.getCityLevelSearch(requests)
            citySearchHTMLList.extend(cityHTML)
            print("Gathered HTML for job postings in {} city-specific queries".format(len(citySearchHTMLList)))
            time.sleep(approxTimeDelay)

        return citySearchHTMLList

    def getJobPostingsInBatches(self, jobLinks: List[str]) -> List[str]:
        jobPostingsHTMLList = []
        #From some manual testing, the rate limit seems to be around 4 requests per second.
        #Raising this rate may lead to data being lost due to rate limiting and 429 status codes
        batchSize = 10
        timeDelaySeconds = 5

        curIndex = 0
        jobLinksCount = len(jobLinks)

        while(curIndex < jobLinksCount):
            batch = jobLinks[curIndex:curIndex+batchSize] if curIndex + batchSize < jobLinksCount else jobLinks[curIndex:]
            batchJobPostingsHTML = self.getJobPostingsHTML(batch)
            curIndex += batchSize
            jobPostingsHTMLList.extend(batchJobPostingsHTML)
            print("Parsed HTML for {} out of {} job postings on LinkedIn".format(len(jobPostingsHTMLList), jobLinksCount))
            time.sleep(timeDelaySeconds)

        return jobPostingsHTMLList

    def getRedirectUrl(self, url: str) -> str:
        try:
            r = requests.get(url) 
            return r.url
        except:
            return url

    def scrapeJobs(self) -> List[JobListing]:
        print("Starting LinkedIn scrape...")
        
        if not self.searchTerms or not self.locations:
            print("searchTerms and locations cannot be empty")
            return None

        time.sleep(1)
        print("Searching for jobs in the following locations: {}".format(", ".join(self.locations)))
        topLevelSearchHTMLList = self.getTopLevelSearch()
        cityCodes = self.getUniqueCityCodes(topLevelSearchHTMLList)

        time.sleep(1)
        print("Narrowing down search to specific LinkedIn city codes: {}".format(", ".join(cityCodes)))
        citySearchHTMLList = self.getCityLevelSearchInBatches(cityCodes)
        jobLinks = self.parseJobLinksFromTopLevelSearches(citySearchHTMLList)
        print("Found {} job links".format(len(jobLinks)))

        time.sleep(1)
        jobPostingsHTMLList = self.getJobPostingsInBatches(jobLinks)
        jobInformation = self.parseJobInformationFromJobLinks(jobPostingsHTMLList)

        for jobListing in jobInformation:
            jobListing.link = self.getRedirectUrl(jobListing.link)
            time.sleep(0.5)

        print("Found {} related job postings on LinkedIn".format(len(jobInformation)))
        sortedJobs = sorted(jobInformation, key=lambda job: job.company)
        return sortedJobs

class GithubScraper:
    def __init__(self, repoURL: str) -> None:
        self.repoURL = repoURL
    
    def scrapeJobs(self) -> List[JobListing]:
        print("Starting Github scrape...")
        jobList = []

        githubInternships = requests.get(self.repoURL)
        soup = BeautifulSoup(githubInternships.text, features="html.parser")

        for internship in soup.select("article table tbody tr"):
            internship_details = internship.find_all("td")
            company = internship_details[0].string
            for detail in internship_details:
                links = detail.find_all("a")
                for link in links:
                    linkURL = link.get("href")
                    newListing = JobListing(company, linkURL)
                    jobList.append(newListing)
        
        sortedJobs = sorted(jobList, key=lambda job: job.company)
        print("Found {} positions on Github".format(len(sortedJobs)))
        return sortedJobs

        