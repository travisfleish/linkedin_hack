#!/usr/bin/env python3
"""
Email Finder - Standalone script to discover business emails from CSV data

This script is completely independent from the LinkedIn scraper and can be
run as a separate process after LinkedIn scraping is complete.

Usage:
    python email_finder.py input.csv
    python email_finder.py input.csv --output output.csv --batch-size 5
"""

import os
import sys
import time
import random
import re
import socket
import smtplib
import argparse
import threading
import logging
from queue import Queue
from urllib.parse import urlparse
from datetime import datetime

import requests
import pandas as pd
import dns.resolver
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Set up logging
def setup_logger(name, log_level="INFO"):
    """Set up and configure logger"""
    # Create logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Set up logger
    logger = logging.getLogger(name)

    # Configure log level
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    logger.setLevel(level_map.get(log_level.upper(), logging.INFO))

    # Set up formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Add file handler
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(f"logs/email_finder_{timestamp}.log")
    file_handler.setFormatter(formatter)

    # Add handlers to logger if not already added
    if not logger.handlers:
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger


# Create main logger
logger = setup_logger("email_finder")


# Rate limiter context manager
class RateLimiter:
    """Rate limiter to prevent API rate limiting"""

    def __init__(self, max_calls_per_minute=10):
        self.min_interval = 60.0 / float(max_calls_per_minute)
        self.last_call_time = 0.0
        self.lock = threading.Lock()

    def __enter__(self):
        """Context manager entry"""
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_call_time

            # If not enough time has passed, sleep
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                logger.debug(f"Rate limit: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)

            self.last_call_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        pass


# Email finder functionality
class EmailFinder:
    """Main email finder class"""

    def __init__(self, log_level="INFO"):
        """Initialize the EmailFinder"""
        self.logger = logger

        # Caches to avoid repeated lookups
        self.domain_cache = {}
        self.mx_cache = {}
        self.email_verification_cache = {}

        # Configure rate limiting
        self.rate_limiter = RateLimiter(
            max_calls_per_minute=int(os.getenv("MAX_EMAIL_CHECKS_PER_MINUTE", "10"))
        )

        # Load API keys
        self.hunter_api_key = os.getenv("HUNTER_API_KEY", "")
        self.email_validator_key = os.getenv("EMAIL_VALIDATOR_KEY", "")

        if self.hunter_api_key:
            self.logger.info("Hunter API key detected")
        if self.email_validator_key:
            self.logger.info("Email Validator API key detected")

    def get_company_domain(self, company_name):
        """
        Get the domain for a company using search engine results

        Args:
            company_name (str): Name of the company

        Returns:
            str: Company domain or None if not found
        """
        if not company_name:
            return None

        # Normalize company name
        company_name = company_name.strip().lower()

        # Check cache first
        if company_name in self.domain_cache:
            self.logger.debug(f"Domain cache hit for {company_name}")
            return self.domain_cache[company_name]

        self.logger.info(f"Discovering domain for: {company_name}")

        try:
            # Apply rate limiting
            with self.rate_limiter:
                # Create search query
                query = f"{company_name} official website"

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }

                # Use DuckDuckGo for search (doesn't require API key)
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
                                        'youtube.com', 'glassdoor.com', 'wikipedia.org', 'crunchbase.com',
                                        'bloomberg.com', 'reuters.com', 'yahoo.com', 'google.com',
                                        'bing.com', 'amazon.com', 'indeed.com']
                        if not any(sd in domain for sd in skip_domains):
                            # Further verify it looks like a company site (not blog, etc.)
                            domain = domain.replace('www.', '')
                            if self._is_likely_company_domain(domain, company_name):
                                potential_domains.append(domain)

                if potential_domains:
                    self.domain_cache[company_name] = potential_domains[0]
                    self.logger.info(f"Found domain via search: {potential_domains[0]}")
                    return potential_domains[0]

                # If no domain found via search, try pattern matching
                clean_name = re.sub(r'[^a-z0-9]', '', company_name.lower())
                potential_domain = f"{clean_name}.com"
                self.domain_cache[company_name] = potential_domain
                self.logger.info(f"Using pattern-based domain: {potential_domain}")
                return potential_domain

        except Exception as e:
            self.logger.error(f"Error discovering domain: {str(e)}")
            return None

    def _is_likely_company_domain(self, domain, company_name):
        """Check if a domain is likely to be a company's official website"""
        # Extract domain name without TLD
        domain_parts = domain.split('.')
        if len(domain_parts) >= 2:
            domain_name = domain_parts[0]

            # Clean company name for comparison
            clean_company = re.sub(r'[^a-z0-9]', '', company_name.lower())

            # Check for similarity
            return (domain_name in clean_company or
                    clean_company in domain_name or
                    self._similarity_score(domain_name, clean_company) > 0.6)
        return False

    def _similarity_score(self, str1, str2):
        """Calculate similarity between two strings"""
        # Simple implementation - using Python's built-in SequenceMatcher
        from difflib import SequenceMatcher
        return SequenceMatcher(None, str1, str2).ratio()

    def generate_email_patterns(self, first_name, last_name, domain):
        """
        Generate likely email patterns based on naming conventions

        Args:
            first_name (str): Person's first name
            last_name (str): Person's last name
            domain (str): Company domain

        Returns:
            list: List of likely email patterns
        """
        if not domain or not first_name or not last_name:
            return []

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

        return patterns

    def verify_email(self, email):
        """
        Verify if an email address exists using DNS MX lookup and SMTP verification

        Args:
            email (str): Email address to verify

        Returns:
            bool: True if the email is valid, False otherwise
        """
        if not email or '@' not in email:
            return False

        # Check cache
        if email in self.email_verification_cache:
            return self.email_verification_cache[email]

        self.logger.debug(f"Verifying email: {email}")

        # Basic syntax check
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            self.logger.debug(f"Invalid email syntax: {email}")
            self.email_verification_cache[email] = False
            return False

        domain = email.split('@')[1]

        # Step 1: Check if MX record exists
        mx_record = self._get_mx_record(domain)
        if not mx_record:
            self.logger.debug(f"No MX record for domain: {domain}")
            self.email_verification_cache[email] = False
            return False

        # Step 2: SMTP verification
        try:
            # Connect to SMTP server
            smtp = smtplib.SMTP(timeout=10)
            smtp.set_debuglevel(0)

            # Try connecting to MX record
            try:
                smtp.connect(mx_record)
            except:
                # Fall back to domain
                smtp.connect(domain)

            smtp.helo(socket.getfqdn())

            # Start TLS if supported
            if smtp.has_extn('STARTTLS'):
                smtp.starttls()
                smtp.ehlo()

            # Use a real-looking email as the sender
            sender = f"verify@{socket.getfqdn()}"

            # Some servers won't let you check without login
            # Just try RCPT command and see what happens
            smtp.mail(sender)
            code, message = smtp.rcpt(email)
            smtp.quit()

            # Return True if successful (code 250 or 251)
            is_valid = code in [250, 251]
            self.email_verification_cache[email] = is_valid
            return is_valid

        except Exception as e:
            self.logger.debug(f"SMTP verification error: {str(e)}")
            # Many servers block verification attempts, so assume the email might be valid
            self.email_verification_cache[email] = True
            return True

    def _get_mx_record(self, domain):
        """Get MX record for a domain"""
        if domain in self.mx_cache:
            return self.mx_cache[domain]

        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            if not mx_records:
                self.mx_cache[domain] = None
                return None

            # Get the MX record with the lowest preference value
            mx_record = sorted(mx_records, key=lambda x: x.preference)[0].exchange
            mx_record = str(mx_record)

            self.mx_cache[domain] = mx_record
            return mx_record

        except Exception as e:
            self.logger.debug(f"Error getting MX record for {domain}: {str(e)}")
            self.mx_cache[domain] = None
            return None

    def find_email_via_api(self, first_name, last_name, domain):
        """
        Try to find email using free API services

        Args:
            first_name (str): Person's first name
            last_name (str): Person's last name
            domain (str): Company domain

        Returns:
            dict: Email discovery result with email and confidence
        """
        result = {"email": None, "confidence": 0}

        # Try Hunter.io if API key provided
        if self.hunter_api_key:
            self.logger.info(f"Trying Hunter.io for {first_name} {last_name} at {domain}")

            try:
                # Apply rate limiting
                with self.rate_limiter:
                    response = requests.get(
                        f"https://api.hunter.io/v2/email-finder?domain={domain}&first_name={first_name}&last_name={last_name}&api_key={self.hunter_api_key}"
                    )
                    data = response.json()

                    if data.get("data", {}).get("email"):
                        email = data["data"]["email"]
                        confidence = data["data"].get("score", 0) * 100  # Convert to 0-100 scale

                        self.logger.info(f"Found email via Hunter.io: {email} (confidence: {confidence:.0f}%)")

                        result["email"] = email
                        result["confidence"] = confidence
                        return result

                self.logger.info("No email found via Hunter.io")

            except Exception as e:
                self.logger.error(f"Hunter.io API error: {str(e)}")

        # Try Email-Validator.net if API key provided
        if self.email_validator_key:
            self.logger.info(f"Trying Email-Validator.net for {first_name} {last_name} at {domain}")

            try:
                # Generate a likely email pattern to check
                email = f"{first_name.lower()}.{last_name.lower()}@{domain}"

                # Apply rate limiting
                with self.rate_limiter:
                    response = requests.get(
                        f"https://api.email-validator.net/api/verify?EmailAddress={email}&APIKey={self.email_validator_key}"
                    )
                    data = response.json()

                    if data.get("status") == 1:
                        confidence = 85  # High confidence if validated

                        self.logger.info(f"Validated email via Email-Validator.net: {email}")

                        result["email"] = email
                        result["confidence"] = confidence
                        return result

                self.logger.info("Email validation failed via Email-Validator.net")

            except Exception as e:
                self.logger.error(f"Email-Validator.net API error: {str(e)}")

        return result

    def search_public_sources(self, first_name, last_name, company_name, domain=None):
        """
        Search public sources for email addresses

        Args:
            first_name (str): Person's first name
            last_name (str): Person's last name
            company_name (str): Company name
            domain (str, optional): Company domain

        Returns:
            dict: Email discovery result with email and confidence
        """
        result = {"email": None, "confidence": 0}

        self.logger.info(f"Searching public sources for {first_name} {last_name} at {company_name}")

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
                self.logger.debug(f"Trying search query: {query}")

                # Apply rate limiting
                with self.rate_limiter:
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
                            if self._is_likely_persons_email(email, first_name, last_name, domain):
                                self.logger.info(f"Found potential email in public sources: {email}")

                                # Set result
                                result["email"] = email
                                result["confidence"] = 60  # Medium confidence for public sources

                                return result

            self.logger.info("No email found in public sources")
            return result

        except Exception as e:
            self.logger.error(f"Error searching public sources: {str(e)}")
            return result

    def _is_likely_persons_email(self, email, first_name, last_name, domain=None):
        """Check if an email is likely to belong to the person"""
        if not email or '@' not in email:
            return False

        email_lower = email.lower()
        first_lower = first_name.lower()
        last_lower = last_name.lower()

        # If domain is specified, check if email matches
        if domain and not email_lower.endswith(f"@{domain}"):
            return False

        # Check if email contains parts of the person's name
        username = email_lower.split('@')[0]

        name_indicators = [
            first_lower,
            last_lower,
            first_lower[0] + last_lower,
            first_lower + last_lower[0],
            first_lower[0] + "." + last_lower,
            first_lower + "." + last_lower,
            first_lower + "_" + last_lower,
            last_lower + "." + first_lower,
            last_lower + first_lower[0]
        ]

        for indicator in name_indicators:
            if indicator in username:
                return True

        return False

    def discover_email(self, first_name, last_name, company_name, linkedin_url=None):
        """
        Main method to discover business email through multiple methods

        Args:
            first_name (str): Person's first name
            last_name (str): Person's last name
            company_name (str): Company name
            linkedin_url (str, optional): LinkedIn profile URL

        Returns:
            dict: Email discovery result
        """
        # Initialize result tracking
        result = {
            "email": None,
            "confidence": 0,  # 0-100 confidence score
            "method": None,  # Which method found the email
            "domain": None  # Company domain
        }

        # Input validation
        if not first_name or not last_name or not company_name:
            self.logger.warning("Missing required input data")
            return result

        self.logger.info(f"Finding email for {first_name} {last_name} at {company_name}")

        # Step 1: Find the company domain
        domain = self.get_company_domain(company_name)

        if not domain:
            self.logger.warning(f"Could not find domain for {company_name}")
            return result

        result["domain"] = domain
        self.logger.info(f"Found domain: {domain}")

        # Step 2: Try API-based discovery first (higher success rate)
        self.logger.info(f"Trying API-based discovery...")

        api_result = self.find_email_via_api(first_name, last_name, domain)

        if api_result and api_result.get("email"):
            result["email"] = api_result["email"]
            result["confidence"] = api_result["confidence"]
            result["method"] = "api"
            self.logger.info(f"Found email via API: {result['email']}")
            return result

        # Step 3: Try pattern-based discovery
        self.logger.info(f"Generating email patterns...")
        email_patterns = self.generate_email_patterns(first_name, last_name, domain)

        # Track failed patterns to potentially use later
        attempted_patterns = []

        for pattern in email_patterns:
            self.logger.debug(f"Testing pattern: {pattern}")

            # Apply rate limiting
            with self.rate_limiter:
                is_valid = self.verify_email(pattern)

            attempted_patterns.append(pattern)

            if is_valid:
                result["email"] = pattern
                result["confidence"] = 75  # Base confidence for pattern matching
                result["method"] = "pattern"
                self.logger.info(f"Found valid email pattern: {pattern}")
                return result

        # Step 4: Try public data sources as a last resort
        self.logger.info(f"Searching public sources...")
        public_result = self.search_public_sources(first_name, last_name, company_name, domain)

        if public_result and public_result.get("email"):
            # Verify the email found in public sources
            with self.rate_limiter:
                is_valid = self.verify_email(public_result["email"])

            if is_valid:
                result["email"] = public_result["email"]
                result["confidence"] = public_result["confidence"]
                result["method"] = "public"
                self.logger.info(f"Found email via public sources: {result['email']}")
                return result

        # Step 5: Last resort - use most common pattern without verification
        if attempted_patterns:
            result["email"] = attempted_patterns[0]  # Use most likely pattern
            result["confidence"] = 30  # Low confidence since not verified
            result["method"] = "unverified_pattern"
            self.logger.info(f"Using most likely unverified pattern: {result['email']}")

        return result

    def process_csv(self, input_file, output_file=None, batch_size=10, num_threads=3, start_row=0):
        """
        Process a CSV file to find emails for each person

        Args:
            input_file (str): Path to input CSV file
            output_file (str, optional): Path to output CSV file
            batch_size (int, optional): Number of profiles to process in each batch
            num_threads (int, optional): Number of worker threads
            start_row (int, optional): Row to start processing from

        Returns:
            str: Path to the output CSV file
        """
        # Set default output file if not provided
        if not output_file:
            base, ext = os.path.splitext(input_file)
            output_file = f"{base}_with_emails{ext}"

        self.logger.info(f"Reading input CSV: {input_file}")

        # Check if file exists
        if not os.path.exists(input_file):
            self.logger.error(f"Input file not found: {input_file}")
            return None

        try:
            # Read the CSV file
            df = pd.read_csv(input_file)
            total_rows = len(df)
            self.logger.info(f"Found {total_rows} rows to process")

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

                for missing in missing_cols[:]:  # Use copy to modify during iteration
                    for alt in alt_cols[missing]:
                        if alt in df.columns:
                            self.logger.info(f"Using '{alt}' for '{missing}'")
                            df[missing] = df[alt]
                            missing_cols.remove(missing)
                            break

            if missing_cols:
                self.logger.error(f"Missing required columns: {', '.join(missing_cols)}")
                self.logger.error(f"Available columns: {', '.join(df.columns)}")
                return None

            # Create email columns if they don't exist
            if "Email" not in df.columns:
                df["Email"] = ""
            if "Email Confidence" not in df.columns:
                df["Email Confidence"] = 0
            if "Email Method" not in df.columns:
                df["Email Method"] = ""
            if "Company Domain" not in df.columns:
                df["Company Domain"] = ""

            # Process in batches
            for batch_start in range(start_row, total_rows, batch_size):
                batch_end = min(batch_start + batch_size, total_rows)
                self.logger.info(f"Processing batch: rows {batch_start} to {batch_end - 1}")

                # Create queue of profiles to process
                queue = Queue()
                results = {}
                lock = threading.Lock()

                # Add profiles to queue
                for i in range(batch_start, batch_end):
                    # Skip if already has email
                    if not pd.isna(df.loc[i, "Email"]) and df.loc[i, "Email"] != "":
                        self.logger.info(f"Row {i + 1}: Already has email, skipping")
                        continue

                    queue.put(i)

                # Define worker function
                def worker():
                    while not queue.empty():
                        try:
                            i = queue.get(block=False)

                            # Extract profile data
                            profile = {
                                "first_name": df.loc[i, "First Name"],
                                "last_name": df.loc[i, "Last Name"],
                                "company": df.loc[i, "Company Name"],
                                "linkedin_url": df.loc[
                                    i, "LinkedIn Profile"] if "LinkedIn Profile" in df.columns else None
                            }

                            # Add random delay to avoid synchronized requests
                            time.sleep(random.uniform(1.0, 3.0))

                            # Discover email
                            email_result = self.discover_email(
                                profile["first_name"],
                                profile["last_name"],
                                profile["company"],
                                profile["linkedin_url"]
                            )

                            # Store result with lock to prevent race conditions
                            with lock:
                                results[i] = email_result

                                # Update dataframe
                                df.loc[i, "Email"] = email_result.get("email", "")
                                df.loc[i, "Email Confidence"] = email_result.get("confidence", 0)
                                df.loc[i, "Email Method"] = email_result.get("method", "")
                                df.loc[i, "Company Domain"] = email_result.get("domain", "")

                                # Save progress immediately
                                df.to_csv(output_file, index=False)

                                email_status = "✅ Found" if email_result.get("email") else "❌ Not found"
                                self.logger.info(f"Row {i + 1}: {email_status} - {email_result.get('email', '')}")

                            # Add random delay between profiles
                            time.sleep(random.uniform(3.0, 8.0))

                        except Exception as e:
                            self.logger.error(f"Error processing row: {str(e)}")
                        finally:
                            if not queue.empty():
                                queue.task_done()

                # Start worker threads
                threads = []
                for _ in range(min(num_threads, queue.qsize())):
                    t = threading.Thread(target=worker)
                    t.daemon = True
                    t.start()
                    threads.append(t)

                # Wait for all threads to complete
                for t in threads:
                    t.join()

                # Save after each batch
                df.to_csv(output_file, index=False)
                self.logger.info(f"Batch completed, saved to {output_file}")

                # Take a break between batches
                if batch_end < total_rows:
                    delay = random.uniform(10, 20)
                    self.logger.info(f"Taking a break for {delay:.2f} seconds between batches...")
                    time.sleep(delay)

            # Final save
            df.to_csv(output_file, index=False)

            # Print statistics
            emails_found = df["Email"].notna() & (df["Email"] != "")
            count = emails_found.sum()
            percentage = count / total_rows * 100 if total_rows > 0 else 0

            self.logger.info(f"Email discovery complete. Results saved to {output_file}")
            self.logger.info(f"Total profiles: {total_rows}")
            self.logger.info(f"Emails found: {count} ({percentage:.2f}%)")

            # Breakdown by confidence level
            high_conf = ((df["Email Confidence"] >= 75) & emails_found).sum()
            med_conf = ((df["Email Confidence"] >= 50) & (df["Email Confidence"] < 75) & emails_found).sum()
            low_conf = ((df["Email Confidence"] < 50) & emails_found).sum()

            self.logger.info(f"High confidence emails: {high_conf} ({high_conf / total_rows * 100:.2f}%)")
            self.logger.info(f"Medium confidence emails: {med_conf} ({med_conf / total_rows * 100:.2f}%)")
            self.logger.info(f"Low confidence emails: {low_conf} ({low_conf / total_rows * 100:.2f}%)")

            return output_file

        except Exception as e:
            self.logger.error(f"Error processing CSV: {str(e)}")
            return None


def main():
    """Main entry point for the email finder"""
    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Email Finder - Discover business emails for LinkedIn profiles"
    )

    # Add arguments
    parser.add_argument(
        "input_file",
        help="Path to CSV file containing profile data"
    )
    parser.add_argument(
        "--output", "-o",
        help="Path to output CSV file (default: input_with_emails.csv)",
        default=None
    )
    parser.add_argument(
        "--batch-size", "-b",
        help="Number of profiles to process in each batch (default: 10)",
        type=int,
        default=10
    )
    parser.add_argument(
        "--threads", "-t",
        help="Number of worker threads to use (default: 3)",
        type=int,
        default=3
    )
    parser.add_argument(
        "--start-row", "-s",
        help="Row to start processing from (default: 0)",
        type=int,
        default=0
    )
    parser.add_argument(
        "--log-level", "-l",
        help="Logging level (default: INFO)",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO"
    )

    # Parse arguments
    args = parser.parse_args()

    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found!")
        sys.exit(1)

    # Set default output file
    if not args.output:
        base, ext = os.path.splitext(args.input_file)
        args.output = f"{base}_with_emails{ext}"

    # Print configuration
    print("\n" + "=" * 70)
    print("Email Finder - Discover business emails for LinkedIn profiles")
    print("=" * 70)
    print(f"Input file: {args.input_file}")
    print(f"Output file: {args.output}")
    print(f"Batch size: {args.batch_size}")
    print(f"Worker threads: {args.threads}")
    print(f"Starting row: {args.start_row}")
    print(f"Log level: {args.log_level}")

    # Check for API keys
    hunter_key = os.getenv("HUNTER_API_KEY", "")
    emailvalidator_key = os.getenv("EMAIL_VALIDATOR_KEY", "")

    if hunter_key:
        print("Hunter.io API key: Found")
    else:
        print("Hunter.io API key: Not found (optional)")

    if emailvalidator_key:
        print("Email-Validator.net API key: Found")
    else:
        print("Email-Validator.net API key: Not found (optional)")

    print("-" * 70)

    # Create email finder instance
    finder = EmailFinder(log_level=args.log_level)

    # Process CSV file
    result = finder.process_csv(
        args.input_file,
        args.output,
        args.batch_size,
        args.threads,
        args.start_row
    )

    if result:
        print("\n" + "=" * 70)
        print(f"Email discovery complete! Results saved to {result}")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("Email discovery failed! Check the logs for details.")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()