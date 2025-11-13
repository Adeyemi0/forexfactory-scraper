from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import logging
import os
import psutil
import gc
from datetime import datetime, timedelta

class ForexFactoryCalendarScraper:
    def __init__(self, csv_file="forex_calendar_data.csv"):
        self.base_url = "https://www.forexfactory.com/calendar"
        self.data = []
        self.csv_file = csv_file
        self.driver = None
        self.failed_dates = []
        self.successful_dates = []
        self.timeout = 30
        self.max_retries = 3

        if os.path.exists(self.csv_file):
            existing_df = pd.read_csv(self.csv_file)
            self.data = existing_df.to_dict("records")
            logging.info(f"Loaded {len(self.data)} records from existing CSV.")

    def initialize_driver(self):
        options = webdriver.EdgeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-logging")
        options.add_argument("--log-level=3")
        options.add_argument("--window-size=1920,1080")
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.javascript": 1,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--disable-application-cache")
        options.add_argument("--disk-cache-size=0")
        options.page_load_strategy = 'normal'

        driver = webdriver.Edge(options=options)
        driver.set_page_load_timeout(self.timeout)
        driver.set_script_timeout(self.timeout)

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                window.chrome = {runtime: {}};
            """
        })
        return driver

    def check_memory(self):
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        if memory_percent > 80:
            logging.warning(f"High memory usage: {memory_percent:.1f}% - forcing cleanup")
            gc.collect()
            time.sleep(2)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            logging.info(f"After cleanup: {memory_percent:.1f}%")
        return memory_percent

    def generate_date_string(self, date_obj):
        month_abbr = date_obj.strftime("%b").lower()
        day = date_obj.strftime("%d")
        year = date_obj.strftime("%Y")
        return f"{month_abbr}{day}.{year}"

    def wait_for_calendar_table(self, timeout=30):
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.calendar__table"))
            )
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.calendar__table tbody"))
            )
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.calendar__table tbody tr"))
            )
            time.sleep(3)
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.calendar__table tbody tr")
            if len(rows) == 0:
                logging.warning("Table loaded but no rows found")
                return False
            logging.info(f"Table loaded with {len(rows)} rows")
            return True
        except TimeoutException as e:
            logging.error(f"Timeout waiting for calendar table: {e}")
            try:
                page_text = self.driver.find_element(By.TAG_NAME, "body").text[:200]
                logging.debug(f"Page content preview: {page_text}")
            except:
                pass
            return False
        except Exception as e:
            logging.error(f"Error waiting for calendar table: {e}")
            return False

    def parse_calendar(self, html, date_str):
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find('table', class_='calendar__table')
        if not table:
            logging.warning("Calendar table not found in HTML")
            return []
        tbody = table.find('tbody')
        if not tbody:
            logging.warning("Table body not found")
            return []

        events = []
        current_date = None
        rows = tbody.find_all('tr')

        for row in rows:
            if 'calendar__row--day-breaker' in row.get('class', []):
                date_cell = row.find('td', class_='calendar__cell')
                if date_cell:
                    current_date = date_cell.get_text(strip=True)
                continue
            if 'calendar__row' not in row.get('class', []):
                continue

            event_data = {'scrape_date': date_str}
            date_cell = row.find('td', class_='calendar__date')
            if date_cell and date_cell.get_text(strip=True):
                event_data['date'] = date_cell.get_text(strip=True)
            else:
                event_data['date'] = current_date if current_date else ''
            time_cell = row.find('td', class_='calendar__time')
            if time_cell:
                time_span = time_cell.find('span')
                event_data['time'] = time_span.get_text(strip=True) if time_span else ''
            else:
                event_data['time'] = ''
            currency_cell = row.find('td', class_='calendar__currency')
            if currency_cell:
                currency_span = currency_cell.find('span')
                event_data['currency'] = currency_span.get_text(strip=True) if currency_span else ''
            else:
                event_data['currency'] = ''
            impact_cell = row.find('td', class_='calendar__impact')
            if impact_cell:
                impact_span = impact_cell.find('span', class_='icon')
                if impact_span:
                    impact_class = impact_span.get('class', [])
                    if 'icon--ff-impact-red' in impact_class:
                        event_data['impact'] = 'High'
                    elif 'icon--ff-impact-ora' in impact_class:
                        event_data['impact'] = 'Medium'
                    elif 'icon--ff-impact-yel' in impact_class:
                        event_data['impact'] = 'Low'
                    else:
                        event_data['impact'] = ''
                else:
                    event_data['impact'] = ''
            else:
                event_data['impact'] = ''
            event_cell = row.find('td', class_='calendar__event')
            if event_cell:
                event_title = event_cell.find('span', class_='calendar__event-title')
                event_data['event'] = event_title.get_text(strip=True) if event_title else ''
            else:
                event_data['event'] = ''
            event_data['has_detail'] = bool(row.get('data-event-id'))
            actual_cell = row.find('td', class_='calendar__actual')
            if actual_cell:
                actual_span = actual_cell.find('span')
                event_data['actual'] = actual_span.get_text(strip=True) if actual_span else ''
            else:
                event_data['actual'] = ''
            forecast_cell = row.find('td', class_='calendar__forecast')
            if forecast_cell:
                forecast_span = forecast_cell.find('span')
                event_data['forecast'] = forecast_span.get_text(strip=True) if forecast_span else ''
            else:
                event_data['forecast'] = ''
            previous_cell = row.find('td', class_='calendar__previous')
            if previous_cell:
                previous_span = previous_cell.find('span')
                event_data['previous'] = previous_span.get_text(strip=True) if previous_span else ''
            else:
                event_data['previous'] = ''
            if event_data.get('event'):
                events.append(event_data)
        return events

    def scrape_date(self, date_str, retry_count=0):
        try:
            url = f"{self.base_url}?day={date_str}"
            logging.info(f"Scraping {date_str} (attempt {retry_count + 1}/{self.max_retries}) - {url}")
            self.driver.get(url)
            if not self.wait_for_calendar_table(timeout=self.timeout):
                raise Exception("Calendar table failed to load")
            html = self.driver.page_source
            events = self.parse_calendar(html, date_str)
            if len(events) == 0:
                logging.warning(f"{date_str} returned 0 events")
                self.failed_dates.append({"date": date_str, "error": "No events found"})
                return [], False
            self.successful_dates.append(date_str)
            logging.info(f"{date_str}: {len(events)} events")
            return events, True
        except Exception as e:
            error_msg = str(e)[:100]
            logging.error(f"{date_str} error: {error_msg}")
            if retry_count < self.max_retries - 1:
                wait_time = random.uniform(5, 10)
                logging.info(f"Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                if retry_count >= 1:
                    logging.info("Reinitializing driver...")
                    try:
                        self.driver.quit()
                    except:
                        pass
                    time.sleep(3)
                    self.driver = self.initialize_driver()
                return self.scrape_date(date_str, retry_count + 1)
            else:
                self.failed_dates.append({"date": date_str, "error": error_msg})
                return [], False

    def scrape_date_range(self, start_date, end_date):
        try:
            current_date = start_date
            total_days = (end_date - start_date).days + 1
            day_count = 0
            consecutive_failures = 0
            while current_date <= end_date:
                day_count += 1
                date_str = self.generate_date_string(current_date)
                logging.info(f"\n{'='*60}")
                logging.info(f"Day {day_count}/{total_days}: {current_date.strftime('%Y-%m-%d')}")
                logging.info(f"{'='*60}")
                memory_percent = self.check_memory()
                logging.info(f"Memory: {memory_percent:.1f}%")
                logging.info(f"Starting browser")
                self.driver = self.initialize_driver()
                try:
                    events, success = self.scrape_date(date_str)
                    if success:
                        self.data.extend(events)
                        self.save_to_csv()
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logging.warning(f"{consecutive_failures} consecutive failures - taking extended break")
                            time.sleep(30)
                            consecutive_failures = 0
                finally:
                    logging.info("Closing browser")
                    try:
                        self.driver.quit()
                    except:
                        pass
                    self.driver = None
                current_date += timedelta(days=1)
                if current_date <= end_date:
                    wait_time = random.uniform(5, 10)
                    logging.info(f"Waiting {wait_time:.1f}s before next date...")
                    time.sleep(wait_time)
        except KeyboardInterrupt:
            logging.info("\nScraping interrupted by user")
        except Exception as e:
            logging.error(f"Fatal error: {e}")
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            self.print_summary()

    def scrape_single_date(self, date_str):
        memory_percent = self.check_memory()
        logging.info(f"Memory: {memory_percent:.1f}%")
        logging.info(f"Starting browser")
        self.driver = self.initialize_driver()
        try:
            events, success = self.scrape_date(date_str)
            if success:
                self.data.extend(events)
                self.save_to_csv()
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            self.print_summary()

    def print_summary(self):
        print("\n" + "="*60)
        print("SCRAPING SUMMARY")
        print("="*60)
        print(f"Successful: {len(self.successful_dates)} dates")
        print(f"Failed: {len(self.failed_dates)} dates")
        print(f"Total events: {len(self.data)}")
        if self.failed_dates:
            print("\nFAILED DATES:")
            for fail in self.failed_dates[:10]:
                print(f"   {fail['date']}: {fail['error']}")
            if len(self.failed_dates) > 10:
                print(f"   ... and {len(self.failed_dates) - 10} more")
        else:
            print("\nAll dates scraped successfully!")
        print("="*60 + "\n")

    def save_to_csv(self):
        df = pd.DataFrame(self.data)
        df.to_csv(self.csv_file, index=False, encoding="utf-8-sig")
        logging.info(f"Saved {len(self.data)} total events to {self.csv_file}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    scraper = ForexFactoryCalendarScraper(csv_file="forex_calendar_data.csv")
    start_date = datetime(2019, 1, 1)
    end_date = datetime(2025, 11, 11)
    scraper.scrape_date_range(start_date, end_date)
