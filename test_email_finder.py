#!/usr/bin/env python3
"""
Email Finder Test Script - Tests email finding for a single profile

This script tests the email finder functionality on a single row from a CSV
or with manually specified profile information.

Usage:
    python email_finder_test.py --csv input.csv --row 5
    python email_finder_test.py --name "John Doe" --company "Acme Inc"
"""

import os
import sys
import time
import re
import socket
import smtplib
import argparse
import logging
from urllib.parse import urlparse

import requests
import pandas as pd
import dns.resolver
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("email_finder_test.log")
    ]
)

logger = logging.getLogger("email_finder_test")


class EmailFinderTest:
    """Email finder test class"""

    def __init__(self):
        """Initialize the test class"""
        logger.info("Initializing email finder test")

        # Caches
        self.domain_cache = {}
        self.mx_cache = {}

        # Load API keys
        self.hunter_api_key = os.getenv("HUNTER_API_KEY", "")
        self.email_validator_key = os.getenv("EMAIL_VALIDATOR_KEY", "")

        if self.hunter_api_key:
            logger.info("Hunter API key detected")
        if self.email_validator_key:
            logger.info("Email Validator API key detected")

    def get_company_domain(self, company_name):
        """Find the domain for a company name"""
        if not company_name:
            return None

        logger.info(f"Step 1: Finding domain for company: {company_name}")

        try:
            # Create search query
            query = f"{company_name} official website"

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            logger.debug(f"Searching for domain using query: {query}")

            # Use DuckDuckGo for search
            response = requests.get(f"https://duckduckgo.com/html/?q={query}", headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract result links
            results = soup.select('.result__url')
            potential_domains = []

            for result in results[:5]:  # Check top 5 results
                url = result.get('href')
                if url:
                    parsed_url = urlparse(url)
                    domain = parsed_url.netloc.lower()

                    # Skip common non-company domains
                    skip_domains = ['linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com',
                                    'youtube.com', 'glassdoor.com', 'wikipedia.org', 'crunchbase.com']
                    if not any(sd in domain for sd in skip_domains):
                        # Remove www. prefix if present
                        domain = domain.replace('www.', '')
                        logger.debug(f"Found potential domain: {domain}")
                        potential_domains.append(domain)

            if potential_domains:
                logger.info(f"Found domain: {potential_domains[0]}")
                return potential_domains[0]

            # Fall back to pattern matching if no domain found
            logger.info("No domain found via search, trying pattern matching")
            clean_name = re.sub(r'[^a-z0-9]', '', company_name.lower())
            potential_domain = f"{clean_name}.com"
            logger.info(f"Using pattern-based domain: {potential_domain}")
            return potential_domain

        except Exception as e:
            logger.error(f"Error finding domain: {str(e)}")
            return None

    def generate_email_patterns(self, first_name, last_name, domain):
        """Generate likely email patterns"""
        if not domain or not first_name or not last_name:
            return []

        logger.info(f"Step 2: Generating email patterns for {first_name} {last_name} at {domain}")

        # Normalize inputs
        first = first_name.lower().strip()
        last = last_name.lower().strip()
        f_initial = first[0] if first else ''
        l_initial = last[0] if last else ''

        # Remove special characters from names
        first = re.sub(r'[^a-z0-9]', '', first)
        last = re.sub(r'[^a-z0-9]', '', last)

        # Common email patterns in order of likelihood
        patterns = [
            f"{first}.{last}@{domain}",  # john.doe@company.com
            f"{first}{last}@{domain}",  # johndoe@company.com
            f"{f_initial}{last}@{domain}",  # jdoe@company.com
            f"{first}@{domain}",  # john@company.com
            f"{first}{l_initial}@{domain}",  # johnd@company.com
            f"{first}-{last}@{domain}",  # john-doe@company.com
            f"{f_initial}.{last}@{domain}",  # j.doe@company.com
            f"{last}.{first}@{domain}",  # doe.john@company.com
            f"{first}_{last}@{domain}",  # john_doe@company.com
            f"{last}{first}@{domain}",  # doejohn@company.com
            f"{last}@{domain}",  # doe@company.com
        ]

        for pattern in patterns:
            logger.debug(f"Generated pattern: {pattern}")

        return patterns

    def verify_email(self, email):
        """Verify if an email address exists"""
        if not email or '@' not in email:
            return False

        logger.info(f"Step 3: Verifying email: {email}")

        # Basic syntax check
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            logger.debug(f"Invalid email syntax: {email}")
            return False

        domain = email.split('@')[1]

        # Check if MX record exists
        try:
            logger.debug(f"Looking up MX records for domain: {domain}")
            mx_records = dns.resolver.resolve(domain, 'MX')
            if not mx_records:
                logger.debug(f"No MX record found for domain: {domain}")
                return False

            mx_record = str(sorted(mx_records, key=lambda x: x.preference)[0].exchange)
            logger.debug(f"Found MX record: {mx_record}")

            # SMTP verification - only enable this if you want aggressive checking
            try:
                logger.debug(f"Attempting SMTP verification (this may be blocked by some servers)")

                # Connect to SMTP server
                smtp = smtplib.SMTP(timeout=10)
                smtp.set_debuglevel(0)

                # Try connecting to the server
                try:
                    logger.debug(f"Connecting to MX server: {mx_record}")
                    smtp.connect(mx_record)
                except:
                    logger.debug(f"Connection to MX failed, trying domain directly: {domain}")
                    smtp.connect(domain)

                logger.debug("Sending HELO")
                smtp.helo(socket.getfqdn())

                # Try STARTTLS if available
                if smtp.has_extn('STARTTLS'):
                    logger.debug("Starting TLS")
                    smtp.starttls()
                    smtp.ehlo()

                # Use a valid-looking sender
                sender = f"verify@{socket.getfqdn()}"

                # Check if recipient exists
                logger.debug(f"Sending MAIL FROM: {sender}")
                smtp.mail(sender)

                logger.debug(f"Sending RCPT TO: {email}")
                code, message = smtp.rcpt(email)
                smtp.quit()

                # Check result code
                logger.debug(f"SMTP response code: {code}, message: {message}")
                is_valid = code in [250, 251]

                if is_valid:
                    logger.info(f"Email verified via SMTP: {email}")
                else:
                    logger.info(f"Email rejected via SMTP: {email}")

                return is_valid

            except Exception as e:
                logger.debug(f"SMTP verification error: {str(e)}")
                # Many servers block verification, so assume email might be valid
                logger.info(f"SMTP verification failed but domain has valid MX record. Assuming email might be valid.")
                return True

        except Exception as e:
            logger.error(f"DNS resolution error: {str(e)}")
            return False

    def try_hunter_api(self, first_name, last_name, domain):
        """Try to find email using Hunter.io API"""
        if not self.hunter_api_key:
            logger.info("Hunter API key not found, skipping API search")
            return None

        logger.info(f"Step 4: Trying Hunter.io API for {first_name} {last_name} at {domain}")

        try:
            url = f"https://api.hunter.io/v2/email-finder?domain={domain}&first_name={first_name}&last_name={last_name}&api_key={self.hunter_api_key}"
            logger.debug(f"Making request to: {url}")

            response = requests.get(url)
            data = response.json()

            logger.debug(f"Hunter API response: {data}")

            if data.get("data", {}).get("email"):
                email = data["data"]["email"]
                confidence = data["data"].get("score", 0) * 100

                logger.info(f"Found email via Hunter.io: {email} (confidence: {confidence:.0f}%)")

                return {
                    "email": email,
                    "confidence": confidence,
                    "method": "hunter_api"
                }

            logger.info("No email found via Hunter.io API")
            return None

        except Exception as e:
            logger.error(f"Hunter.io API error: {str(e)}")
            return None

    def search_public_sources(self, first_name, last_name, company_name, domain=None):
        """Search public sources for email addresses"""
        logger.info(f"Step 5: Searching public sources for {first_name} {last_name} at {company_name}")

        try:
            # Create search queries
            queries = [
                f'"{first_name} {last_name}" email {company_name}',
                f'"{first_name} {last_name}" contact {company_name}'
            ]

            if domain:
                queries.append(f'"{first_name} {last_name}" @{domain}')

            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            for query in queries:
                logger.debug(f"Trying search query: {query}")

                response = requests.get(f"https://duckduckgo.com/html/?q={query}", headers=headers)
                soup = BeautifulSoup(response.text, 'html.parser')

                # Extract search results
                search_results = soup.select('.result__body')

                # Process each search result
                for result_item in search_results:
                    result_text = result_item.get_text()

                    # Look for email pattern in result
                    email_pattern = r'[\w\.-]+@[\w\.-]+'
                    emails = re.findall(email_pattern, result_text)

                    for email in emails:
                        # Check if email might belong to the person
                        username = email.split('@')[0].lower()
                        first_lower = first_name.lower()
                        last_lower = last_name.lower()

                        if (first_lower in username or
                                last_lower in username or
                                first_lower[0] + last_lower in username or
                                first_lower + last_lower[0] in username):
                            logger.info(f"Found potential email in public sources: {email}")

                            return {
                                "email": email,
                                "confidence": 60,
                                "method": "public_search"
                            }

            logger.info("No email found in public sources")
            return None

        except Exception as e:
            logger.error(f"Error searching public sources: {str(e)}")
            return None

    def find_email(self, first_name, last_name, company_name):
        """Main method to find an email address"""
        logger.info(f"Finding email for {first_name} {last_name} at {company_name}")

        # Step 1: Find company domain
        domain = self.get_company_domain(company_name)
        if not domain:
            logger.error("Could not find company domain, aborting")
            return {
                "email": None,
                "confidence": 0,
                "method": None,
                "domain": None
            }

        # Step 2: Try API search first (if available)
        api_result = self.try_hunter_api(first_name, last_name, domain)
        if api_result:
            api_result["domain"] = domain
            return api_result

        # Step 3: Try email patterns with verification
        patterns = self.generate_email_patterns(first_name, last_name, domain)

        for pattern in patterns:
            if self.verify_email(pattern):
                return {
                    "email": pattern,
                    "confidence": 75,
                    "method": "pattern_verification",
                    "domain": domain
                }

        # Step 4: Try public search
        public_result = self.search_public_sources(first_name, last_name, company_name, domain)
        if public_result:
            public_result["domain"] = domain
            return public_result

        # Step 5: Fall back to most probable pattern without verification
        if patterns:
            return {
                "email": patterns[0],
                "confidence": 30,
                "method": "unverified_pattern",
                "domain": domain
            }

        return {
            "email": None,
            "confidence": 0,
            "method": None,
            "domain": domain
        }


def process_csv_row(csv_file, row_number):
    """Process a single row from a CSV file"""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)

        # Check if row number is valid
        if row_number < 0 or row_number >= len(df):
            logger.error(f"Invalid row number: {row_number}. CSV has {len(df)} rows (0-{len(df) - 1})")
            return False

        # Get row data
        row = df.iloc[row_number]

        # Check for required columns
        required_cols = ["First Name", "Last Name", "Company Name"]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            # Try alternative column names
            alt_cols = {
                "First Name": ["FirstName", "Given Name", "Name"],
                "Last Name": ["LastName", "Surname", "Family Name"],
                "Company Name": ["Company", "Organization", "Employer"]
            }

            for missing in missing_cols[:]:
                found = False
                for alt in alt_cols[missing]:
                    if alt in df.columns:
                        logger.info(f"Using '{alt}' for '{missing}'")
                        if missing == "First Name":
                            first_name = row[alt]
                            found = True
                            break
                        elif missing == "Last Name":
                            last_name = row[alt]
                            found = True
                            break
                        elif missing == "Company Name":
                            company_name = row[alt]
                            found = True
                            break

                if not found:
                    logger.error(f"Could not find alternative for {missing}")
                    return False
        else:
            first_name = row["First Name"]
            last_name = row["Last Name"]
            company_name = row["Company Name"]

        # Create email finder and find email
        finder = EmailFinderTest()
        result = finder.find_email(first_name, last_name, company_name)

        # Print results
        print("\n" + "=" * 70)
        print(f"Email Finder Test Results - Row {row_number}")
        print("=" * 70)
        print(f"Name: {first_name} {last_name}")
        print(f"Company: {company_name}")
        print(f"Company Domain: {result['domain']}")
        print(f"Email: {result['email']}")
        print(f"Confidence: {result['confidence']}%")
        print(f"Method: {result['method']}")
        print("=" * 70)

        return True

    except Exception as e:
        logger.error(f"Error processing CSV row: {str(e)}")
        return False


def process_manual_input(name, company):
    """Process manually entered name and company"""
    try:
        # Split name into first and last name
        name_parts = name.split()
        if len(name_parts) < 2:
            logger.error("Name must include both first and last name")
            return False

        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:])

        # Create email finder and find email
        finder = EmailFinderTest()
        result = finder.find_email(first_name, last_name, company)

        # Print results
        print("\n" + "=" * 70)
        print(f"Email Finder Test Results - Manual Input")
        print("=" * 70)
        print(f"Name: {first_name} {last_name}")
        print(f"Company: {company}")
        print(f"Company Domain: {result['domain']}")
        print(f"Email: {result['email']}")
        print(f"Confidence: {result['confidence']}%")
        print(f"Method: {result['method']}")
        print("=" * 70)

        return True

    except Exception as e:
        logger.error(f"Error processing manual input: {str(e)}")
        return False


def main():
    """Main entry point for the email finder test script"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Email Finder Test - Test email finding for a single profile"
    )

    # Create mutually exclusive groups for input
    input_group = parser.add_mutually_exclusive_group(required=True)

    input_group.add_argument(
        "--csv",
        help="Path to CSV file containing profile data"
    )

    input_group.add_argument(
        "--name",
        help="Full name of the person (e.g., 'John Doe')"
    )

    # Additional arguments
    parser.add_argument(
        "--row",
        help="Row number in CSV file (0-based index)",
        type=int,
        default=0
    )

    parser.add_argument(
        "--company",
        help="Company name (required if using --name)"
    )

    # Parse arguments
    args = parser.parse_args()

    # Process based on input type
    if args.csv:
        # Check if file exists
        if not os.path.exists(args.csv):
            print(f"Error: CSV file not found: {args.csv}")
            return 1

        print(f"Processing row {args.row} from {args.csv}")
        success = process_csv_row(args.csv, args.row)
    else:
        # Manual input - check for company name
        if not args.company:
            print("Error: --company is required when using --name")
            return 1

        print(f"Processing manual input: {args.name} at {args.company}")
        success = process_manual_input(args.name, args.company)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())