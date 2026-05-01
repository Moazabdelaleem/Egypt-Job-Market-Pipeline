import scrapy


class BiJobsItem(scrapy.Item):
    """Extended item model capturing all useful job-listing fields."""
    title        = scrapy.Field()
    company      = scrapy.Field()
    location     = scrapy.Field()
    experience   = scrapy.Field()
    job_type     = scrapy.Field()
    salary       = scrapy.Field()
    career_level = scrapy.Field()
    date_posted  = scrapy.Field()
    historical_date = scrapy.Field() # Calculated Date (e.g. 2024-02-28)
    keywords     = scrapy.Field()   # skills / tags
    description  = scrapy.Field()   # first 500 chars of job description
    url          = scrapy.Field()
    scraped_at   = scrapy.Field()   # UTC timestamp of when we scraped it
    skills       = scrapy.Field()   # extracted technical skills (list)
