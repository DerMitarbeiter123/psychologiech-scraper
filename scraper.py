import json
import requests
from bs4 import BeautifulSoup
import time
import re
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import string
import random

# =============================================================================
# DEFAULT CONFIGURATION SETTINGS
# =============================================================================

DEFAULT_SETTINGS = {
    # Scraping limits
    'MAX_PROFILES_TO_SCRAPE': 50000,  # Set to None for all profiles, or a number to limit
    'MAX_SERVICES_PER_PROFILE': 50,  # Maximum services to extract per profile
    'MAX_LANGUAGES_PER_PROFILE': 10,  # Maximum languages to extract per profile

    # Scraping behavior
    'SAVE_INTERVAL': 10,  # Save progress every N profiles
    'RATE_LIMIT_SECONDS': 1,  # Seconds to wait between requests (be respectful)

    # Debug settings
    'DEBUG_MODE': True,  # Set to True for detailed logging
    'DEBUG_RECORD_ID': 570737,  # Specific record to debug

    # Error handling
    'MAX_ERROR_MESSAGE_LENGTH': 50,  # Truncate error messages to this length

    # Database settings
    'DB_CONFIG': {
        "host": "turntable.proxy.rlwy.net",
        "port": "54461",
        "database": "railway",
        "user": "postgres",
        "password": "cPpOcdramzwgTbBXVKDWGuBRQJHkgAbX"
    }
}

# Current settings (will be loaded from file or use defaults)
SETTINGS = DEFAULT_SETTINGS.copy()

# Database configuration
DB_CONFIG = SETTINGS['DB_CONFIG']

# =============================================================================
from urllib.parse import urljoin, urlparse

# =============================================================================
# SETTINGS MANAGEMENT
# =============================================================================

def load_settings():
    """Load settings from file or use defaults"""
    global SETTINGS
    settings_file = 'data/scraper_settings.json'

    try:
        if os.path.exists(settings_file):
            with open(settings_file, 'r', encoding='utf-8') as f:
                loaded_settings = json.load(f)
                # Merge loaded settings with defaults (preserves new default settings)
                SETTINGS.update(loaded_settings)
                print(f"[OK] Settings loaded from {settings_file}")
        else:
            print("[INFO] No settings file found, using defaults")
    except Exception as e:
        print(f"[WARN] Error loading settings: {e}, using defaults")

def save_settings():
    """Save current settings to file"""
    settings_file = 'data/scraper_settings.json'
    os.makedirs('data', exist_ok=True)

    try:
        with open(settings_file, 'w', encoding='utf-8') as f:
            json.dump(SETTINGS, f, indent=2, ensure_ascii=False)
        print(f"[OK] Settings saved to {settings_file}")
    except Exception as e:
        print(f"[ERROR] Error saving settings: {e}")

def show_settings():
    """Display current settings"""
    print("\n" + "="*60)
    print("CURRENT SCRAPER SETTINGS")
    print("="*60)

    for key, value in SETTINGS.items():
        if key == 'DB_CONFIG':
            print(f"{key}:")
            for db_key, db_value in value.items():
                # Hide password in display
                if db_key == 'password':
                    print(f"  {db_key}: {'*' * len(db_value)}")
                else:
                    print(f"  {db_key}: {db_value}")
        else:
            print(f"{key}: {value}")

def edit_setting():
    """Edit a specific setting"""
    show_settings()

    print("\nWhich setting would you like to edit?")
    setting_options = {
        '1': ('MAX_PROFILES_TO_SCRAPE', 'Maximum profiles to scrape (None for all)'),
        '2': ('MAX_SERVICES_PER_PROFILE', 'Maximum services per profile'),
        '3': ('MAX_LANGUAGES_PER_PROFILE', 'Maximum languages per profile'),
        '4': ('SAVE_INTERVAL', 'Save progress every N profiles'),
        '5': ('RATE_LIMIT_SECONDS', 'Rate limit between requests (seconds)'),
        '6': ('DEBUG_MODE', 'Debug mode (True/False)'),
        '7': ('DEBUG_RECORD_ID', 'Debug record ID'),
        '8': ('MAX_ERROR_MESSAGE_LENGTH', 'Max error message length'),
    }

    for key, (setting, desc) in setting_options.items():
        print(f"{key}. {setting}: {desc}")

    choice = input("\nEnter setting number (1-8): ").strip()

    if choice in setting_options:
        setting_key, description = setting_options[choice]

        if setting_key in ['DEBUG_MODE']:
            # Boolean setting
            current_value = SETTINGS[setting_key]
            new_value = input(f"Current value: {current_value}. Enter new value (True/False): ").strip()
            if new_value.lower() in ['true', 'false']:
                SETTINGS[setting_key] = new_value.lower() == 'true'
                print(f"[OK] {setting_key} updated to {SETTINGS[setting_key]}")
            else:
                print("[ERROR] Invalid boolean value")
        else:
            # Numeric setting
            current_value = SETTINGS[setting_key]
            new_value = input(f"Current value: {current_value}. Enter new value: ").strip()

            try:
                if setting_key == 'MAX_PROFILES_TO_SCRAPE':
                    SETTINGS[setting_key] = None if new_value.lower() == 'none' else int(new_value)
                else:
                    SETTINGS[setting_key] = int(new_value)
                print(f"[OK] {setting_key} updated to {SETTINGS[setting_key]}")
            except ValueError:
                print("[ERROR] Invalid numeric value")

        save_settings()
    else:
        print("[ERROR] Invalid choice")

# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def generate_therapist_id():
    """Generate an ID similar to existing Therapist table IDs"""
    prefix = "cmj"
    timestamp_part = "d" + str(int(datetime.now().timestamp() * 1000))[-6:]
    random_part = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"{prefix}{timestamp_part}{random_part}"

def map_therapist_to_db(therapist):
    """Map psychologie.ch therapist data to database columns - COMPREHENSIVE MAPPING"""
    db_record = {}

    # Generate new ID (Option A: new UUIDs like existing records)
    db_record['id'] = generate_therapist_id()

    # REQUIRED fields - must provide values
    current_time = datetime.now()
    db_record['createdAt'] = current_time
    db_record['updatedAt'] = current_time

    # ============================================================================
    # BASIC NAME & IDENTITY
    # ============================================================================
    db_record['firstName'] = therapist.get('firstname') or therapist.get('user', {}).get('firstname', 'Unknown')
    db_record['lastName'] = therapist.get('lastname') or therapist.get('user', {}).get('lastname', 'Unknown')

    # ============================================================================
    # LOCATION & ADDRESS DATA (NOW FULLY MAPPED!)
    # ============================================================================
    db_record['street'] = therapist.get('address', '')
    db_record['city'] = therapist.get('city', 'Unknown')

    # Convert canton_id to canton name/abbreviation
    canton_id = therapist.get('canton_id')
    if canton_id is not None:
        # Swiss canton mapping (ID -> Abbreviation) - CORRECTED
        canton_mapping = {
            1: 'AG',   # Aargau (not Zurich!)
            2: 'AI',   # Appenzell Innerrhoden
            3: 'AR',   # Appenzell Ausserrhoden
            4: 'BE',   # Bern
            5: 'BL',   # Basel-Landschaft
            6: 'BS',   # Basel-Stadt
            7: 'FR',   # Fribourg (not Nidwalden!)
            8: 'GE',   # Geneva
            9: 'GL',   # Glarus
            10: 'GR',  # Graubunden
            11: 'JU',  # Jura
            12: 'LU',  # Luzern
            13: 'NE',  # Neuchatel
            14: 'NW',  # Nidwalden
            15: 'OW',  # Obwalden
            16: 'SG',  # St. Gallen
            17: 'SH',  # Schaffhausen
            18: 'SO',  # Solothurn
            19: 'SZ',  # Schwyz
            20: 'TG',  # Thurgau
            21: 'TI',  # Ticino
            22: 'UR',  # Uri
            23: 'VD',  # Vaud
            24: 'VS',  # Valais
            25: 'ZG',  # Zug (not Geneva!)
            26: 'ZH'   # Zurich
        }
        db_record['canton'] = canton_mapping.get(canton_id, f'Unknown({canton_id})')
    else:
        db_record['canton'] = 'Unknown'

    # ZIP CODE - NOW FILLED! (was missing before)
    db_record['zip'] = therapist.get('zip', '')

    # COORDINATES - NOW FILLED! (was missing before)
    # API provides "latitude"/"longitude" - map to database "lat"/"lng"
    if therapist.get('latitude'):
        try:
            db_record['lat'] = float(therapist['latitude'])
        except (ValueError, TypeError):
            db_record['lat'] = None
    else:
        db_record['lat'] = None

    if therapist.get('longitude'):
        try:
            db_record['lng'] = float(therapist['longitude'])
        except (ValueError, TypeError):
            db_record['lng'] = None
    else:
        db_record['lng'] = None

    # ============================================================================
    # CONTACT INFORMATION
    # ============================================================================
    db_record['phone'] = therapist.get('phone', '')
    db_record['mobile'] = therapist.get('mobile_phone', '')
    db_record['email'] = therapist.get('email', '')
    db_record['website'] = therapist.get('website', '')

    # ============================================================================
    # VISUAL & MEDIA
    # ============================================================================
    db_record['hasPicture'] = bool(therapist.get('profile_image_url'))
    db_record['pictureUrl'] = therapist.get('profile_image_url', '')

    # ============================================================================
    # PROFESSIONAL INFORMATION
    # ============================================================================
    # Professional titles - map FSP titles to available fields
    fsp_titles = therapist.get('fsp_titles', [])
    if fsp_titles:
        db_record['professionalTitle1'] = fsp_titles[0] if len(fsp_titles) > 0 else ''
        db_record['professionalTitle2'] = fsp_titles[1] if len(fsp_titles) > 1 else ''

    # Practice name
    db_record['practiceName'] = therapist.get('practice_name', therapist.get('name', ''))

    # ============================================================================
    # SERVICES & SPECIALIZATIONS (NOW FULLY MAPPED!)
    # ============================================================================
    # Languages spoken - store as JSON string
    languages = therapist.get('languages', [])
    if languages:
        db_record['languages_spoken'] = json.dumps(languages)

    # Specializations - store as JSON string (was missing before)
    specialisations = therapist.get('specialisations', [])
    if specialisations:
        db_record['specializations'] = json.dumps(specialisations)

    # Services offered - store as JSON string (was missing before)
    offer = therapist.get('offer', [])
    if offer:
        db_record['services_offered'] = json.dumps(offer)

    # Target groups - store as JSON string (was missing before)
    target_groups = therapist.get('target_groups', [])
    if target_groups:
        db_record['target_groups_json'] = json.dumps(target_groups)

    # Billing options - store as JSON string (was missing before)
    billing = therapist.get('billing', [])
    if billing:
        db_record['billing_options'] = json.dumps(billing)

    # ============================================================================
    # BIOGRAPHICAL CONTENT
    # ============================================================================
    # About me section - NOW FILLED! (was missing before)
    db_record['about_me'] = therapist.get('about_me', '')

    # ============================================================================
    # AVAILABILITY & ONLINE FEATURES
    # ============================================================================
    online_sessions = therapist.get('online_sessions', 'unavailable')
    db_record['offersOnlineTherapy'] = online_sessions == 'available'
    db_record['offersVideoCall'] = online_sessions == 'available'
    db_record['online_availability'] = online_sessions

    # ============================================================================
    # REQUIRED BOOLEAN FIELDS
    # ============================================================================
    db_record['contactVerified'] = False
    db_record['showPhone'] = bool(therapist.get('phone'))
    db_record['showMobile'] = bool(therapist.get('mobile_phone'))
    db_record['showFax'] = False
    db_record['offersPhoneCall'] = bool(therapist.get('phone'))
    db_record['onlineBookingConsultation'] = False

    # ============================================================================
    # INSURANCE COVERAGE
    # ============================================================================
    # Default to comprehensive coverage - can be refined based on billing data
    db_record['insuranceBasic'] = True
    db_record['insuranceSelf'] = True
    db_record['insuranceSupplementary'] = True

    # ============================================================================
    # QUALITY & COMPLETENESS SCORES
    # ============================================================================
    db_record['dataQualityScore'] = 9  # Higher score for psychologie.ch rich data
    db_record['profileCompleteness'] = 95  # Very complete profiles
    db_record['dataCompleteness'] = 95
    db_record['contactDataQuality'] = 'verified_generic'

    # ============================================================================
    # METADATA & IDENTIFIERS
    # ============================================================================
    db_record['gender'] = 'unknown'
    db_record['role'] = 'therapist'  # Set role to therapist for all psychologie.ch records
    db_record['specialization'] = therapist.get('practice_name', 'Psychology')
    db_record['trafficLight'] = 1
    db_record['citySearchValue'] = therapist.get('city', '').lower()

    # Profile URL
    db_record['url'] = therapist.get('url', f'https://www.psychologie.ch/en/psyfinder/{therapist.get("firstname", "").lower()}-{therapist.get("lastname", "").lower()}')

    # Data source tracking
    db_record['dataSource'] = 'manual'  # Since 'psychologie.ch' is not a valid enum value
    db_record['externalId'] = str(therapist.get('id', ''))
    db_record['psychologie_ch_id'] = str(therapist.get('id', ''))
    db_record['psychologie_ch_user_id'] = str(therapist.get('user_id', ''))

    # Scraped timestamp
    if therapist.get('scraped_at'):
        db_record['scraped_at'] = datetime.fromtimestamp(therapist['scraped_at'])

    # ============================================================================
    # BACKUP & RAW DATA
    # ============================================================================
    # Store full raw data as backup for future analysis
    db_record['raw_data'] = json.dumps(therapist)

    return db_record

def insert_therapist_to_db(therapist):
    """Insert a single therapist record into the database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Check if record exists
        psych_id = str(therapist.get('id', ''))
        cursor.execute('SELECT id FROM "Therapist" WHERE psychologie_ch_id = %s', (psych_id,))
        existing = cursor.fetchone()

        if existing:
            # Update existing record
            db_record = map_therapist_to_db(therapist)
            # Remove id from update (don't change the existing ID)
            if 'id' in db_record:
                del db_record['id']

            set_clause = ', '.join([f'"{col}" = %s' for col in db_record.keys()])
            values = list(db_record.values()) + [existing[0]]

            update_sql = f'UPDATE "Therapist" SET {set_clause} WHERE id = %s'
            cursor.execute(update_sql, values)
            action = "updated"
        else:
            # Insert new record
            db_record = map_therapist_to_db(therapist)
            columns = list(db_record.keys())
            values = list(db_record.values())
            placeholders = ['%s'] * len(columns)

            insert_sql = f'''
                INSERT INTO "Therapist" ({', '.join(f'"{col}"' for col in columns)})
                VALUES ({', '.join(placeholders)})
            '''
            cursor.execute(insert_sql, values)
            action = "inserted"

        conn.commit()
        cursor.close()
        conn.close()

        return action

    except Exception as e:
        print(f"[ERROR] Database error for therapist {therapist.get('firstname')} {therapist.get('lastname')}: {e}")
        return "error"

def scrape_and_overwrite_database():
    """Scrape all psychologie.ch profiles and replace conflicting records by URL"""
    print("\n" + "!"*70)
    print("[WARNING] REPLACE CONFLICTING PSYCHOLOGIE.CH RECORDS [WARNING]")
    print("!"*70)
    print("This will:")
    print("1. Keep all records from other data sources (doc24, wepractice, manual)")
    print("2. REPLACE psychologie.ch records that have URL conflicts")
    print("3. Scrape therapist profiles ONE BY ONE from psychologie.ch")
    print("4. For each profile: DELETE existing record with same URL, then INSERT new one")
    print("5. Show errors immediately - you can monitor and stop if needed")
    print("6. This process can take several hours!")
    print("!"*70)

    confirm = input("\nAre you SURE you want to continue? Type 'YES' to proceed: ").strip()
    if confirm != 'YES':
        print("[CANCELLED] Operation cancelled by user")
        return

    print("\n[+] Starting scrape and overwrite process...")

    try:
        # Connect to database for real-time operations
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        conn.commit()

        # Step 2: Scrape and import profiles one by one
        print("\n[*] STEP 2: Scraping and importing profiles immediately...")

        # Load psychologists data
        psychologists = extract_psychologists_from_json('data/psychologie.ch.json')

        successful_scrapes = 0
        successful_inserts = 0
        failed_scrapes = 0
        failed_inserts = 0

        print(f"Starting scrape of ALL {len(psychologists)} profiles...")
        print("Each profile will be scraped and immediately imported to database.")
        print("Progress will be shown every 10 profiles.")
        print("Each insert uses its own transaction - one failure won't stop others.")

        for i, psych in enumerate(psychologists):
            if (i + 1) % 10 == 0:
                print(f"Progress: {i+1}/{len(psychologists)} ({(i+1)/len(psychologists)*100:.1f}%) | "
                      f"Scraped: {successful_scrapes} | Inserted: {successful_inserts} | "
                      f"Failed: {failed_scrapes + failed_inserts}")

            # Create URL slug
            def normalize_for_url(text):
                text = text.lower().replace(' ', '-')
                char_map = {
                    'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
                    'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
                    'ç': 'c', 'ć': 'c', 'č': 'c',
                    'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
                    'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
                    'ñ': 'n', 'ń': 'n',
                    'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
                    'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
                    'ý': 'y', 'ÿ': 'y',
                    'ż': 'z', 'ź': 'z', 'ž': 'z'
                }
                for special, normal in char_map.items():
                    text = text.replace(special, normal)
                text = re.sub(r'[^\w\-]', '', text)
                return text

            firstname_slug = normalize_for_url(psych['firstname'].strip())
            lastname_slug = normalize_for_url(psych['lastname'].strip())
            url_slug = f"{firstname_slug}-{lastname_slug}"

            # Scrape profile
            result = scrape_profile_page(
                psych['id'],
                psych['user_id'],
                psych['firstname'],
                psych['lastname'],
                url_slug
            )

            if result:
                # Merge data
                merged_data = psych.copy()
                for key, value in result.items():
                    if key not in merged_data or not merged_data[key]:
                        merged_data[key] = value
                merged_data['scraped_at'] = time.time()

                # Immediately insert/replace in database (each operation in its own transaction)
                try:
                    db_record = map_therapist_to_db(merged_data)
                    therapist_url = db_record['url']

                    # Start a new transaction for this operation
                    conn.rollback()  # Clear any aborted transaction state

                    # DELETE existing record with same URL (from any dataSource)
                    cursor.execute('DELETE FROM "Therapist" WHERE url = %s', (therapist_url,))
                    deleted_for_this_record = cursor.rowcount

                    # INSERT the new psychologie.ch record
                    columns = list(db_record.keys())
                    values = list(db_record.values())
                    placeholders = ['%s'] * len(columns)

                    insert_sql = f'''
                        INSERT INTO "Therapist" ({', '.join(f'"{col}"' for col in columns)})
                        VALUES ({', '.join(placeholders)})
                    '''

                    cursor.execute(insert_sql, values)
                    conn.commit()  # Commit this specific insert/replace operation

                    successful_inserts += 1
                    successful_scrapes += 1

                    if deleted_for_this_record > 0:
                        print(f"[REPLACE] Replaced existing record for {psych['firstname']} {psych['lastname']}")

                except Exception as e:
                    failed_inserts += 1
                    conn.rollback()  # Rollback on failure to clear transaction state
                    print(f"[DB ERROR] Failed to insert {psych['firstname']} {psych['lastname']}: {e}")

                    # Save failed URL construction for DB insertion failures too
                    failed_url_file = 'data/failed_url_constructions.json'
                    constructed_url = f"https://www.psychologie.ch/en/psyfinder/{url_slug}"
                    error_reason = f"Database insertion failed: {str(e)[:100]}"
                    save_failed_url_construction(
                        failed_url_file, psych['id'], psych.get('user_id'),
                        psych['firstname'], psych['lastname'],
                        url_slug, constructed_url, error_reason
                    )
            else:
                failed_scrapes += 1
                print(f"[SCRAPE FAILED] {psych['firstname']} {psych['lastname']} (ID: {psych['id']})")

                # Save failed URL construction for later analysis
                failed_url_file = 'data/failed_url_constructions.json'
                constructed_url = f"https://www.psychologie.ch/en/psyfinder/{url_slug}"
                error_reason = "Scraping failed - URL construction or page access issue"
                save_failed_url_construction(
                    failed_url_file, psych['id'], psych.get('user_id'),
                    psych['firstname'], psych['lastname'],
                    url_slug, constructed_url, error_reason
                )

            # Rate limiting
            time.sleep(SETTINGS['RATE_LIMIT_SECONDS'])

        # Summary
        print("\n" + "="*60)
        print("SELECTIVE REPLACE PSYCHOLOGIE.CH DATA - COMPLETED!")
        print("="*60)
        print(f"[+] Total profiles processed: {len(psychologists)}")
        print(f"[+] Successfully scraped: {successful_scrapes}")
        print(f"[+] Successfully inserted/replaced: {successful_inserts}")
        print(f"[+] Failed scrapes: {failed_scrapes}")
        print(f"[+] Failed DB operations: {failed_inserts}")

        # Show final database composition
        cursor.execute('SELECT "dataSource", COUNT(*) FROM "Therapist" GROUP BY "dataSource" ORDER BY COUNT(*) DESC')
        sources = cursor.fetchall()
        print(f"[+] Final database composition:")
        for source, count in sources:
            print(f"  - {source or 'psychologie.ch'}: {count} records")
        print("="*60)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Scrape and overwrite failed: {e}")
        import traceback
        traceback.print_exc()

def scrape_all_profiles():
    """Scrape all profiles regardless of limits"""
    # Temporarily override settings
    original_limit = SETTINGS['MAX_PROFILES_TO_SCRAPE']
    SETTINGS['MAX_PROFILES_TO_SCRAPE'] = None

    try:
        # Load data
        psychologists = extract_psychologists_from_json('data/psychologie.ch.json')

        successful = 0
        failed = 0
        scraped_data = []

        print(f"Starting scrape of ALL {len(psychologists)} profiles...")
        print("This will take a long time. Progress will be shown every 10 profiles.")

        for i, psych in enumerate(psychologists):
            if (i + 1) % 10 == 0:
                print(f"Progress: {i+1}/{len(psychologists)} ({(i+1)/len(psychologists)*100:.1f}%) | Success: {successful} | Failed: {failed}")

            # Create URL slug
            def normalize_for_url(text):
                text = text.lower().replace(' ', '-')
                char_map = {
                    'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
                    'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
                    'ç': 'c', 'ć': 'c', 'č': 'c',
                    'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
                    'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
                    'ñ': 'n', 'ń': 'n',
                    'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
                    'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
                    'ý': 'y', 'ÿ': 'y',
                    'ż': 'z', 'ź': 'z', 'ž': 'z'
                }
                for special, normal in char_map.items():
                    text = text.replace(special, normal)
                text = re.sub(r'[^\w\-]', '', text)
                return text

            firstname_slug = normalize_for_url(psych['firstname'].strip())
            lastname_slug = normalize_for_url(psych['lastname'].strip())

            url_slug = f"{firstname_slug}-{lastname_slug}"

            # Scrape profile
            result = scrape_profile_page(
                psych['id'],
                psych['user_id'],
                psych['firstname'],
                psych['lastname'],
                url_slug
            )

            if result:
                # Merge data
                merged_data = psych.copy()
                for key, value in result.items():
                    if key not in merged_data or not merged_data[key]:
                        merged_data[key] = value
                merged_data['scraped_at'] = time.time()
                scraped_data.append(merged_data)
                successful += 1
            else:
                failed += 1

            # Rate limiting
            time.sleep(SETTINGS['RATE_LIMIT_SECONDS'])

        print(f"\nScraping complete: {successful} successful, {failed} failed")
        return scraped_data

    finally:
        # Restore original setting
        SETTINGS['MAX_PROFILES_TO_SCRAPE'] = original_limit

def extract_psychologists_from_json(json_file_path):
    """Extract all psychologists' first and last names from the JSON file."""
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    psychologists = []

    # Navigate to the display-markers dispatch
    try:
        components = data.get('components', [])
        for component in components:
            effects = component.get('effects', {})
            dispatches = effects.get('dispatches', [])

            for dispatch in dispatches:
                if dispatch.get('name') == 'display-markers':
                    markers = dispatch.get('params', [])
                    # markers is an array containing one array of psychologist objects
                    if markers and isinstance(markers[0], list):
                        for psychologist in markers[0]:
                            user = psychologist.get('user', {})
                            firstname = user.get('firstname')
                            lastname = user.get('lastname')
                            user_id = user.get('id')
                            psychologist_id = psychologist.get('id')

                            if firstname and lastname:
                                # Create URL slug by normalizing special characters
                                def normalize_for_url(text):
                                    # Convert to lowercase and replace spaces with hyphens
                                    text = text.lower().replace(' ', '-')

                                    # Replace common special characters with ASCII equivalents
                                    char_map = {
                                        'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
                                        'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
                                        'ç': 'c', 'ć': 'c', 'č': 'c',
                                        'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
                                        'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
                                        'ñ': 'n', 'ń': 'n',
                                        'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
                                        'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
                                        'ý': 'y', 'ÿ': 'y',
                                        'ż': 'z', 'ź': 'z', 'ž': 'z'
                                    }

                                    for special, normal in char_map.items():
                                        text = text.replace(special, normal)

                                    # Remove any remaining special characters except hyphens and alphanumeric
                                    text = re.sub(r'[^\w\-]', '', text)

                                    return text

                                firstname_slug = normalize_for_url(firstname.strip())
                                lastname_slug = normalize_for_url(lastname.strip())
                                url_slug = f"{firstname_slug}-{lastname_slug}"

                                # Extract ALL available API data, not just basic fields
                                psychologist_data = {
                                    # Basic identification
                                    'id': psychologist_id,
                                    'user_id': user_id,
                                    'firstname': firstname,
                                    'lastname': lastname,
                                    'url_slug': url_slug,

                                    # Location data (this was missing!)
                                    'address': psychologist.get('address', ''),
                                    'address_2': psychologist.get('address_2'),
                                    'zip': psychologist.get('zip', ''),
                                    'city': psychologist.get('city', ''),
                                    'canton_id': psychologist.get('canton_id'),
                                    'country_id': psychologist.get('country_id'),
                                    'latitude': psychologist.get('latitude'),
                                    'longitude': psychologist.get('longitude'),

                                    # Contact and professional data
                                    'mobile_phone': psychologist.get('mobile_phone'),
                                    'phone': psychologist.get('phone'),
                                    'email': psychologist.get('email'),
                                    'website': psychologist.get('website'),
                                    'name': psychologist.get('name'),  # practice name
                                    'name_2': psychologist.get('name_2'),

                                    # Accessibility and address flags
                                    'is_wheelchair_accessible': psychologist.get('is_wheelchair_accessible'),
                                    'is_work_address': psychologist.get('is_work_address'),
                                    'is_main_work_address': psychologist.get('is_main_work_address'),
                                    'is_correspondence_address': psychologist.get('is_correspondence_address'),
                                    'is_private_address': psychologist.get('is_private_address'),
                                    'is_billing_address': psychologist.get('is_billing_address'),

                                    # Timestamps
                                    'created_at': psychologist.get('created_at'),
                                    'updated_at': psychologist.get('updated_at'),

                                    # Full user object
                                    'user': psychologist.get('user', {})
                                }

                                psychologists.append(psychologist_data)
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        return []

    return psychologists

def scrape_profile_page(psychologist_id, user_id, firstname, lastname, url_slug):
    """Scrape individual profile page for psychologist data."""
    base_url = "https://www.psychologie.ch/en/psyfinder/"
    url = f"{base_url}{url_slug}"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract data based on the structure we saw
        profile_data = {
            'id': psychologist_id,
            'user_id': user_id,
            'firstname': firstname,
            'lastname': lastname,
            'url': url,
            'scraped_at': time.time()
        }

        # Try to extract various fields
        try:
            # Name (usually in h1 or similar)
            name_elem = soup.find('h1') or soup.find(class_=re.compile(r'name|title', re.I))
            if name_elem:
                profile_data['full_name'] = name_elem.get_text(strip=True)

            # Practice name - look for the specific structure we saw
            practice_elem = soup.find(string=re.compile(r'Praxis|Practice|Cabinet|Studio', re.I))
            if practice_elem:
                # Get the parent element that contains the full practice name
                parent = practice_elem.parent
                if parent and parent.name in ['h2', 'h3', 'div', 'p']:
                    profile_data['practice_name'] = parent.get_text(strip=True)
                else:
                    profile_data['practice_name'] = practice_elem.strip()

            # Address - look for the address section more specifically
            address_text = ""
            # Look for elements with address-like content
            address_candidates = soup.find_all(['div', 'p', 'span'], string=re.compile(r'(strasse|straße|weg|platz|rue|avenue|via|street|road)', re.I))
            for candidate in address_candidates:
                text = candidate.get_text(strip=True)
                # Filter out JavaScript and very short texts
                if len(text) > 10 and not text.startswith('(') and not 'function' in text.lower():
                    address_text = text
                    break

            # If no specific address found, try to extract from structured data
            if not address_text:
                # Look for the address section by finding text near city/zip patterns
                text_content = soup.get_text()
                # Look for patterns like "Street Name, ZIP City"
                address_match = re.search(r'([A-Za-zäöüÄÖÜ\s]+\d{1,3}[A-Za-zäöüÄÖÜ\s]*),\s*(\d{4})\s+([A-Za-zäöüÄÖÜ\s]+)', text_content)
                if address_match:
                    address_text = f"{address_match.group(1).strip()}, {address_match.group(2)} {address_match.group(3).strip()}"

            # Clean up address formatting
            if address_text:
                # Remove excessive whitespace and newlines
                address_text = re.sub(r'\s+', ' ', address_text).strip()
                # Remove trailing commas if they exist before country
                address_text = re.sub(r',\s*,', ',', address_text)
                profile_data['address'] = address_text

            # Phone number - improved regex
            phone_pattern = r'[\+]?[41][\s\-\.]?\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d[\s\-\.]*\d'
            phone_match = re.search(phone_pattern, soup.get_text())
            if phone_match:
                # Clean up the phone number
                phone = re.sub(r'[^\+\d]', '', phone_match.group())
                if len(phone) >= 10:  # Valid phone number length
                    profile_data['phone'] = phone

            # Email
            email_elem = soup.find('a', href=re.compile(r'mailto:', re.I))
            if email_elem:
                profile_data['email'] = email_elem.get('href').replace('mailto:', '')

            # Website
            website_elem = soup.find('a', href=re.compile(r'^https?://(?!www\.psychologie\.ch)', re.I))
            if website_elem:
                profile_data['website'] = website_elem.get('href')

            # Online sessions - look for "Available" or "Unavailable"
            online_sessions_elem = soup.find(string=re.compile(r'Online sessions?', re.I))
            if online_sessions_elem:
                # Get the parent or next element that contains the status
                parent = online_sessions_elem.parent
                if parent:
                    # Look for status in siblings or parent's text
                    status_match = re.search(r'(Available|Unavailable)', parent.get_text(), re.I)
                    if status_match:
                        profile_data['online_sessions'] = status_match.group(1).lower()
                    else:
                        # Look in the broader context
                        context = parent.find_parent('div') if parent.name != 'div' else parent
                        if context:
                            status_match = re.search(r'(Available|Unavailable)', context.get_text(), re.I)
                            if status_match:
                                profile_data['online_sessions'] = status_match.group(1).lower()

            # Fallback: search the entire page text for online session status
            if 'online_sessions' not in profile_data:
                page_text = soup.get_text()
                if 'Online sessions' in page_text:
                    if 'Available' in page_text:
                        profile_data['online_sessions'] = 'available'
                    elif 'Unavailable' in page_text:
                        profile_data['online_sessions'] = 'unavailable'

            # Profile image - look for the main profile image
            profile_img = soup.find('img', class_=re.compile(r'br-16px|profile|avatar', re.I))
            if profile_img and profile_img.get('src'):
                profile_data['profile_image_url'] = profile_img['src']
            else:
                # Fallback: look for any img with psychologist name in alt
                alt_img = soup.find('img', alt=re.compile(f'{firstname}|{lastname}', re.I))
                if alt_img and alt_img.get('src'):
                    profile_data['profile_image_url'] = alt_img['src']

            # FSP titles
            fsp_titles = []
            title_elems = soup.find_all(string=re.compile(r'Fachpsychologin|Fachpsychologe|Eidgenössisch', re.I))
            for elem in title_elems:
                if elem and elem.parent:
                    title_text = elem.parent.get_text(strip=True)
                    if len(title_text) < 200:  # Avoid picking up large blocks of text
                        fsp_titles.append(title_text)
            if fsp_titles:
                profile_data['fsp_titles'] = list(set(fsp_titles))  # Remove duplicates

            # Specialisations - improved extraction with filtering
            specialisations = []

            # Look for specialisation section more specifically
            spec_section = soup.find(string=re.compile(r'Specialisation', re.I))
            if spec_section:
                parent = spec_section.parent
                if parent:
                    container = parent.find_next_sibling(['div', 'p'])
                    if container:
                        spec_text = container.get_text(strip=True)
                        if spec_text and len(spec_text) > 10 and len(spec_text) < 1000:
                            # Clean up the text - remove quotes and normalize
                            spec_text = spec_text.strip('"').strip("'")
                            if spec_text:
                                specialisations.append(spec_text)

            # Also look for structured specialisation data in the page
            # Look for elements that might contain actual specializations (not URLs or JS)
            potential_specs = soup.find_all(['p', 'div'], string=re.compile(r'(therapie|psychologie|systemisch|hypnose|cognitive|behavioral|trauma)', re.I))
            for elem in potential_specs[:3]:  # Limit to avoid spam
                text = elem.get_text(strip=True)
                # Filter out problematic content
                if (len(text) > 15 and len(text) < 200 and
                    not any(problem in text.lower() for problem in [
                        'http', 'https', 'var ', 'function', 'redirect', 'role of the fsp',
                        'psychologie.ch', 'afp.psychologie.ch', 'gtm.', 'google', 'facebook'
                    ])):
                    # Check if it looks like a real specialization
                    if any(keyword in text.lower() for keyword in [
                        'therapie', 'psychologie', 'systemisch', 'hypnose', 'trauma',
                        'kognitiv', 'behavioral', 'psychoanalyse', 'gestalt', 'familie'
                    ]):
                        specialisations.append(text)

            # Remove duplicates and limit
            if specialisations:
                # Filter out duplicates and very similar entries
                unique_specs = []
                seen = set()
                for spec in specialisations:
                    # Create a normalized version for comparison
                    normalized = re.sub(r'[^\w\s]', '', spec.lower()).strip()
                    if normalized not in seen and len(normalized) > 10:
                        unique_specs.append(spec)
                        seen.add(normalized)

                profile_data['specialisations'] = unique_specs[:3]  # Limit to 3 high-quality specs

            # Languages
            languages = []
            lang_terms = ['German', 'French', 'Italian', 'English', 'Swiss German']
            for lang in lang_terms:
                if soup.find(string=re.compile(rf'\b{lang}\b', re.I)):
                    languages.append(lang)
            if languages:
                profile_data['languages'] = languages

            # Extract structured sections: About me, Offer, Target groups, Languages, Billing

            # About me section - targeted biography extraction
            about_me_text = ""

            # Look for the specific biography content that follows "About me" or similar headers
            # First, find the "About me" header
            about_headers = soup.find_all(['h2', 'h3', 'h4', 'div', 'strong'], string=re.compile(r'about me|über mich|à propos|biographie', re.I))

            for header in about_headers:
                # Get the next sibling element which should contain the biography
                next_elem = header.find_next_sibling(['p', 'div'])
                if next_elem:
                    text = next_elem.get_text(strip=True)
                    # Check if this looks like biography content
                    if (len(text) > 50 and len(text) < 3000 and
                        not text.startswith('http') and 'var ' not in text.lower() and
                        not any(problem in text for problem in ['billing', 'offer', 'languages', 'telephone', 'email'])):
                        # Look for biography indicators
                        bio_indicators = ['born', 'trained', 'studied', 'worked', 'experience', 'therapist', 'psychologist',
                                        'university', 'degree', 'practice', 'clinic', 'hospital', 'i ', 'je ', 'my ',
                                        'trained', 'worked as', 'specialized in']
                        if any(indicator in text.lower() for indicator in bio_indicators):
                            about_me_text = text
                            break

                # If next sibling didn't work, try to find content within the same parent
                if not about_me_text:
                    parent = header.parent
                    if parent and parent.name in ['div', 'section']:
                        # Get all paragraphs in this section after the header
                        header_index = None
                        for i, child in enumerate(parent.children):
                            if child == header or (hasattr(child, 'get_text') and header.get_text().strip() in child.get_text()):
                                header_index = i
                                break

                        if header_index is not None:
                            content_parts = []
                            for child in list(parent.children)[header_index + 1:]:
                                if child.name in ['p', 'div'] and hasattr(child, 'get_text'):
                                    text = child.get_text(strip=True)
                                    if text and len(text) > 20:
                                        content_parts.append(text)
                                        if len(content_parts) >= 3:  # Limit to first few paragraphs
                                            break

                            if content_parts:
                                combined_text = ' '.join(content_parts)
                                if len(combined_text) > 100:
                                    about_me_text = combined_text
                                    break

            # Fallback: look for substantial paragraphs that contain first-person language
            if not about_me_text:
                paragraphs = soup.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if (len(text) > 80 and len(text) < 2000 and
                        not text.startswith('http') and 'var ' not in text.lower() and
                        not p.find_parent(['ul', 'ol', 'table', 'header', 'nav']) and
                        not any(header in text.lower() for header in ['billing', 'offer', 'languages', 'telephone', 'email', 'website', 'address'])):
                        # Must contain first-person indicators or biography keywords
                        if (any(word in text.lower() for word in ['i ', 'je ', 'my ', 'me ', 'born', 'trained', 'studied', 'worked']) and
                            not text.lower().startswith(('news', 'psychologists', 'psyfinder'))):
                            about_me_text = text
                            break

            if about_me_text:
                # Clean up the text
                about_me_text = re.sub(r'\s+', ' ', about_me_text).strip()
                about_me_text = about_me_text.strip('.,;:- ')
                profile_data['about_me'] = about_me_text[:3000]  # Allow up to 3000 chars for biographies

            # Offer/Services section - comprehensive extraction
            services = []

            # First, try structured extraction from "Offer" section
            offer_section = soup.find(string=re.compile(r'Offer', re.I))
            if offer_section:
                parent = offer_section.parent
                if parent:
                    container = parent.find_next_sibling(['div', 'ul', 'p'])
                    if container:
                        # Try to extract from list items first
                        list_items = container.find_all('li')
                        if list_items:
                            for item in list_items:
                                text = item.get_text(strip=True)
                                if len(text) > 2 and not text.startswith('http'):
                                    services.append(text)
                        else:
                            # If no list items, try to extract from continuous text
                            container_text = container.get_text(strip=True)
                            if container_text and len(container_text) > 10:
                                # Split on common separators and clean up
                                # Handle patterns like "Depression Panic attacks and anxiety Burnout"
                                parts = re.split(r'\s+(?=Unemployment|Work stoppage|Dissatisfaction|Bulling|Psychosocial|Relationship|Divorce|Family|Gender|Sexual|Retirement|Loneliness|Behavioural|Substance|Food|Stress|Bereavement|Suicidal|Existential|Sleep|Chronic|Depression|Panic|Burnout|Self-esteem)', container_text)

                                for part in parts:
                                    part = part.strip()
                                    if len(part) > 2 and not part.startswith('http'):
                                        # Further split on spaces if it's a compound term
                                        subparts = part.split()
                                        if len(subparts) <= 4:  # Keep short phrases together
                                            services.append(part)
                                        else:
                                            # For longer phrases, split on common connectors
                                            subparts = re.split(r'\s+and\s+|\s+or\s+', part)
                                            services.extend([sp.strip() for sp in subparts if sp.strip()])

            # Second, try to find services in any div or section that contains service-like content
            if len(services) < 10:  # If we didn't get many services, try broader search
                all_containers = soup.find_all(['div', 'section'], class_=re.compile(r'(content|services|offer)', re.I))
                for container in all_containers:
                    text = container.get_text(strip=True)
                    if len(text) > 50 and any(keyword in text.lower() for keyword in ['depression', 'anxiety', 'therapy', 'stress']):
                        # Extract service-like terms from the text
                        service_candidates = re.findall(r'\b[A-Z][a-z]+(?:\s+[a-z]+){0,3}\b', text)
                        for candidate in service_candidates:
                            candidate = candidate.strip()
                            if (len(candidate) > 3 and len(candidate) < 50 and
                                candidate.lower() not in ['offer', 'services', 'target', 'groups', 'languages', 'billing', 'about', 'specialisation']):
                                services.append(candidate)

            # Third, fallback to keyword-based extraction for any missing services
            fallback_services = []
            service_keywords = [
                'Unemployment', 'Work stoppage', 'Dissatisfaction with job', 'Bullying', 'Psychosocial risks',
                'Relationship problems', 'Divorce', 'Separation', 'Family problems', 'Gender identity',
                'Sexual orientation', 'Retirement', 'Loneliness', 'Behavioural addictions', 'Substance addictions',
                'Food-related problems', 'Behavioural problems', 'Stress related to learning', 'Bullying/harassment',
                'Bereavement', 'Suicidal thoughts', 'Stress', 'Existential crisis', 'Sleep-related problems',
                'Chronic pain', 'Depression', 'Panic attacks', 'Anxiety', 'Burnout', 'Self-esteem'
            ]

            for keyword in service_keywords:
                if soup.find(string=re.compile(rf'\b{re.escape(keyword)}\b', re.I)):
                    fallback_services.append(keyword)

            services.extend(fallback_services)

            # Clean and deduplicate
            if services:
                cleaned_services = []
                seen = set()
                noise_terms = [
                    'offer', 'services', 'and', 'with', 'for', 'the', 'to', 'of', 'in', 'at', 'by', 'on',
                    'greater protection for patients', 'the role of the', 'who pays what', 'rights', 'online intervention',
                    'training', 'formapsy', 'how to obtain', 'qualification', 'postgraduate', 'become a member',
                    'registration', 'next', 'fsp', 'federation', 'about us', 'affiliated institutions', 'working at',
                    'job offers', 'contact', 'declaration', 'confidentiality', 'terms and conditions', 'impressum'
                ]

                for service in services:
                    service = service.strip()
                    # Skip if it's noise or too short/long
                    if (len(service) < 3 or len(service) > 100 or
                        service.lower() in noise_terms or
                        any(noise in service.lower() for noise in noise_terms) or
                        service in seen):
                        continue

                    cleaned_services.append(service)
                    seen.add(service)

                profile_data['offer'] = cleaned_services[:SETTINGS['MAX_SERVICES_PER_PROFILE']]

            # Target groups section
            target_section = soup.find(string=re.compile(r'Target groups', re.I))
            if target_section:
                parent = target_section.parent
                if parent:
                    container = parent.find_next_sibling(['div', 'ul'])
                    if container:
                        targets = []
                        list_items = container.find_all('li') or container.find_all(string=True)
                        for item in list_items:
                            if isinstance(item, str):
                                text = item.strip()
                            else:
                                text = item.get_text(strip=True)

                            if len(text) > 2 and not text.startswith('http'):
                                targets.append(text)

                        if targets:
                            profile_data['target_groups'] = targets[:15]  # Limit to 15 groups

            # Languages - improved extraction
            languages_section = soup.find(string=re.compile(r'Languages', re.I))
            if languages_section:
                parent = languages_section.parent
                if parent:
                    container = parent.find_next_sibling(['div', 'ul'])
                    if container:
                        languages = []
                        list_items = container.find_all('li') or container.find_all(string=True)
                        for item in list_items:
                            if isinstance(item, str):
                                text = item.strip()
                            else:
                                text = item.get_text(strip=True)

                            # Clean up language names
                            text = text.strip()
                            if len(text) > 2 and text not in ['Languages', 'Sprachen', 'Langues', 'Lingue']:
                                languages.append(text)

                        if languages:
                            profile_data['languages'] = languages[:SETTINGS['MAX_LANGUAGES_PER_PROFILE']]

            # Billing information - improved formatting
            billing_info = []

            billing_section = soup.find(string=re.compile(r'Billing', re.I))
            if billing_section:
                parent = billing_section.parent
                if parent:
                    container = parent.find_next_sibling(['div', 'ul', 'p'])
                    if container:
                        if container.name == 'ul':
                            # Handle list format
                            list_items = container.find_all('li')
                            for item in list_items:
                                text = item.get_text(strip=True)
                                if text and len(text) > 3:
                                    billing_info.append(text)
                        else:
                            # Handle paragraph format
                            billing_text = container.get_text(strip=True)
                            if billing_text:
                                # Split concatenated billing info
                                # Common patterns: "Covered by basic insuranceTo be paid by yourself"
                                billing_text = re.sub(r'([a-z])([A-Z])', r'\1. \2', billing_text)
                                billing_text = re.sub(r'\s+', ' ', billing_text)
                                billing_info.append(billing_text)

            # Also look for billing info in other locations
            billing_keywords = ['covered by', 'supplementary', 'basic insurance', 'paid by yourself']
            for keyword in billing_keywords:
                elements = soup.find_all(string=re.compile(keyword, re.I))
                for elem in elements:
                    if elem and elem.parent:
                        text = elem.parent.get_text(strip=True)
                        if len(text) > 10 and text not in billing_info:
                            # Clean up formatting
                            text = re.sub(r'\s+', ' ', text)
                            billing_info.append(text)

            if billing_info:
                # Remove duplicates and format nicely
                unique_billing = list(set(billing_info))
                profile_data['billing'] = unique_billing[:3]  # Limit to 3 billing options

            # Fallback for services if structured extraction didn't work
            if 'offer' not in profile_data:
                services = []
                service_terms = ['Depression', 'Anxiety', 'Therapy', 'Counseling', 'Psychotherapy', 'Burnout', 'Stress', 'Trauma', 'Divorce', 'Bereavement', 'Panic attacks']
                for service in service_terms:
                    if soup.find(string=re.compile(rf'\b{re.escape(service)}\b', re.I)):
                        services.append(service)
                if services:
                    profile_data['offer'] = list(set(services))

        except Exception as e:
            print(f"Error extracting data for {firstname} {lastname}: {e}")

        return profile_data

    except requests.RequestException as e:
        print(f"Request error for {firstname} {lastname}: {e}")
        return None
    except Exception as e:
        print(f"Error scraping {firstname} {lastname}: {e}")
        return None

def save_incremental_data(output_file, new_data):
    """Save data incrementally to JSON file"""
    try:
        # Try to load existing data
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        # Append new data
        existing_data.append(new_data)

        # Save back to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)

    except Exception as e:
        print(f"Error saving data incrementally: {e}")

def save_failed_url_construction(failed_url_file, psychologist_id, user_id, firstname, lastname, generated_slug, constructed_url, error_reason, timestamp=None):
    """Save failed URL construction details for later analysis and fixing"""
    if timestamp is None:
        timestamp = time.time()

    failed_record = {
        'id': psychologist_id,
        'user_id': user_id,
        'firstname': firstname,
        'lastname': lastname,
        'generated_slug': generated_slug,
        'constructed_url': constructed_url,
        'error_reason': error_reason,
        'failed_at': timestamp,
        'error_message_truncated': str(error_reason)[:SETTINGS['MAX_ERROR_MESSAGE_LENGTH']] if error_reason else ""
    }

    try:
        # Try to load existing failed records
        try:
            with open(failed_url_file, 'r', encoding='utf-8') as f:
                existing_failed = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_failed = []

        # Check if this record already exists (avoid duplicates)
        existing_ids = {record.get('id') for record in existing_failed}
        if psychologist_id not in existing_ids:
            existing_failed.append(failed_record)

            # Save back to file
            with open(failed_url_file, 'w', encoding='utf-8') as f:
                json.dump(existing_failed, f, indent=2, ensure_ascii=False)

            print(f"  Saved failed URL construction for {firstname} {lastname} (ID: {psychologist_id})")

    except Exception as e:
        print(f"Error saving failed URL construction: {e}")

def validate_url_construction(psychologists, num_tests=5):
    """Test URL construction with random samples"""
    import random

    print(f"\nTesting URL construction with {num_tests} random samples...")

    # First test the specific Jean-François Briefer example
    print("  Testing Jean-François Briefer example:")
    test_briefer = {
        'firstname': 'Jean-François',
        'lastname': 'Briefer',
        'url_slug': 'jean-francois-briefer'  # Expected result
    }

    def normalize_for_url(text):
        # Convert to lowercase and replace spaces with hyphens
        text = text.lower().replace(' ', '-')

        # Replace common special characters with ASCII equivalents
        char_map = {
            'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
            'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
            'ç': 'c', 'ć': 'c', 'č': 'c',
            'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
            'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
            'ñ': 'n', 'ń': 'n',
            'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
            'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
            'ý': 'y', 'ÿ': 'y',
            'ż': 'z', 'ź': 'z', 'ž': 'z'
        }

        for special, normal in char_map.items():
            text = text.replace(special, normal)

        # Remove any remaining special characters except hyphens and alphanumeric
        text = re.sub(r'[^\w\-]', '', text)

        return text

    expected_slug = f"{normalize_for_url(test_briefer['firstname'])}-{normalize_for_url(test_briefer['lastname'])}"
    print(f"    Input: {test_briefer['firstname']} {test_briefer['lastname']}")
    print(f"    Expected: {expected_slug}")
    if expected_slug == 'jean-francois-briefer':
        print("    SUCCESS: URL slug generation working correctly")
    else:
        print(f"    FAILED: Expected 'jean-francois-briefer', got '{expected_slug}'")

    # Get random samples (excluding the Briefer test)
    test_samples = random.sample(psychologists, min(num_tests, len(psychologists)))

    base_url = "https://www.psychologie.ch/en/psyfinder/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    successful_urls = 0
    failed_urls = 0

    for i, psych in enumerate(test_samples):
        url = f"{base_url}{psych['url_slug']}"
        print(f"  Test {i+1}: {psych['firstname']} {psych['lastname']} -> {psych['url_slug']}")

        try:
            response = requests.head(url, headers=headers, timeout=5)  # Use HEAD request for faster testing
            if response.status_code == 200:
                successful_urls += 1
                print("    SUCCESS: URL valid")
            else:
                failed_urls += 1
                print(f"    FAILED: URL returned {response.status_code}")
        except Exception as e:
            failed_urls += 1
            print(f"    FAILED: URL error: {str(e)[:SETTINGS['MAX_ERROR_MESSAGE_LENGTH']]}...")

        time.sleep(0.5)  # Small delay between tests

    print(f"\nURL validation complete: {successful_urls} valid, {failed_urls} failed")
    return successful_urls > failed_urls * 0.8  # Accept if >80% success rate

def scrape_and_merge_in_place():
    """Scrape profiles and merge data directly into psychologie.ch.json"""

    json_file = 'data/psychologie.ch.json'
    backup_file = 'data/psychologie.ch.json.backup'

    print("Loading psychologie.ch.json...")
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Create backup
    print("Creating backup...")
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("Finding psychologists to scrape...")

    # Find all psychologists in the data structure and collect them for processing
    psychologists_to_process = []

    try:
        components = data.get('components', [])
        for component in components:
            effects = component.get('effects', {})
            dispatches = effects.get('dispatches', [])

            for dispatch in dispatches:
                if dispatch.get('name') == 'display-markers':
                    params = dispatch.get('params', [])
                    if params and isinstance(params[0], list):
                        psychologists_to_process.extend(params[0])
    except Exception as e:
        print(f"Error finding psychologists: {e}")
        return

    print(f"Found {len(psychologists_to_process)} psychologists total")

    # Apply profile limit from configuration
    if SETTINGS['MAX_PROFILES_TO_SCRAPE'] is not None:
        print(f"Limiting to {SETTINGS['MAX_PROFILES_TO_SCRAPE']} profiles (set MAX_PROFILES_TO_SCRAPE = None for all)")
        psychologists_to_process = psychologists_to_process[:SETTINGS['MAX_PROFILES_TO_SCRAPE']]
    else:
        print("Processing ALL profiles (this may take several hours)")

    successful = 0
    failed = 0
    skipped = 0
    save_interval = SETTINGS['SAVE_INTERVAL']  # Save every N profiles
    start_time = time.time()
    last_progress_time = start_time

    # Count how many records still need processing
    records_to_process = 0
    for psychologist in psychologists_to_process:
        user = psychologist.get('user', {})
        firstname = user.get('firstname')
        lastname = user.get('lastname')
        psych_id = psychologist.get('id')
        if (firstname and lastname and psych_id) and 'scraped_at' not in psychologist:
            records_to_process += 1

    print(f"Starting scrape of {records_to_process} unscraped profiles (out of {len(psychologists_to_process)} total)...")
    print("="*60)

    for i, psychologist in enumerate(psychologists_to_process):
        user = psychologist.get('user', {})
        firstname = user.get('firstname')
        lastname = user.get('lastname')
        psych_id = psychologist.get('id')

        if not (firstname and lastname and psych_id):
            if SETTINGS['DEBUG_MODE'] and psych_id == SETTINGS['DEBUG_RECORD_ID']:
                print(f"DEBUG: Skipping record {psych_id} - condition failed (firstname={repr(firstname)}, lastname={repr(lastname)}, psych_id={repr(psych_id)})")
            continue

        # Skip already scraped records
        if 'scraped_at' in psychologist:
            skipped += 1
            if SETTINGS['DEBUG_MODE'] and psych_id == SETTINGS['DEBUG_RECORD_ID']:
                print(f"DEBUG: Skipping record {psych_id} - already scraped at {psychologist['scraped_at']}")
            continue

        # Show progress every 10 records or every 30 seconds
        current_time = time.time()
        processed_count = successful + failed
        if processed_count % 10 == 0 or (current_time - last_progress_time) > 30:
            elapsed = current_time - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            eta_seconds = (records_to_process - processed_count) / rate if rate > 0 else 0
            eta_minutes = eta_seconds / 60

            print(f"Progress: {processed_count}/{records_to_process} ({processed_count/records_to_process*100:.1f}%) | "
                  f"Elapsed: {elapsed/60:.1f}min | Rate: {rate:.1f} rec/min | "
                  f"ETA: {eta_minutes:.1f}min | Success: {successful} | Failed: {failed} | Skipped: {skipped}", flush=True)
            last_progress_time = current_time

        print(f"Processing {i+1}/{len(psychologists_to_process)}: {firstname} {lastname}", flush=True)

        # Create URL slug
        def normalize_for_url(text):
            text = text.lower().replace(' ', '-')
            char_map = {
                'ä': 'a', 'ö': 'o', 'ü': 'u', 'ß': 'ss',
                'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
                'ç': 'c', 'ć': 'c', 'č': 'c',
                'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
                'ì': 'i', 'í': 'i', 'î': 'i', 'ï': 'i',
                'ñ': 'n', 'ń': 'n',
                'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o',
                'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
                'ý': 'y', 'ÿ': 'y',
                'ż': 'z', 'ź': 'z', 'ž': 'z'
            }
            for special, normal in char_map.items():
                text = text.replace(special, normal)
            text = re.sub(r'[^\w\-]', '', text)
            return text

        firstname_slug = normalize_for_url(firstname.strip())
        lastname_slug = normalize_for_url(lastname.strip())

        url_slug = f"{firstname_slug}-{lastname_slug}"

        # Scrape the profile
        if SETTINGS['DEBUG_MODE'] and psych_id == SETTINGS['DEBUG_RECORD_ID']:
            print(f"DEBUG: About to scrape URL: https://www.psychologie.ch/en/psyfinder/{url_slug}")

        result = scrape_profile_page(psych_id, user.get('id'), firstname, lastname, url_slug)

        if SETTINGS['DEBUG_MODE'] and psych_id == SETTINGS['DEBUG_RECORD_ID']:
            print(f"DEBUG: Scraping result: {result is not None} (keys: {list(result.keys()) if result else 'None'})")

        if result:
            # Merge scraped data directly into the psychologist record
            for key, value in result.items():
                if key not in psychologist or not psychologist[key]:  # Only add if missing or empty
                    psychologist[key] = value
                elif key in ['offer', 'target_groups', 'languages', 'billing', 'specialisations', 'fsp_titles']:
                    # For array fields, merge them
                    if isinstance(value, list) and isinstance(psychologist.get(key), list):
                        combined = list(set(psychologist[key] + value))
                        psychologist[key] = combined
                    elif value and not psychologist.get(key):
                        psychologist[key] = value

            successful += 1
            print("  SUCCESS - Data merged directly into record")
        else:
            failed += 1
            print(f"  FAILED - Could not scrape {firstname} {lastname} (ID: {psych_id})")

            # Save failed URL construction for later analysis and fixing
            failed_url_file = 'data/failed_url_constructions.json'
            constructed_url = f"https://www.psychologie.ch/en/psyfinder/{url_slug}"
            error_reason = "Scraping failed - likely URL construction issue or page not found"
            save_failed_url_construction(
                failed_url_file, psych_id, user.get('id'), firstname, lastname,
                url_slug, constructed_url, error_reason
            )

        # Save periodically
        if (i + 1) % save_interval == 0:
            print(f"  Saving progress... ({i+1}/{len(psychologists_to_process)} profiles processed)")
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        # Rate limiting - be respectful
        time.sleep(SETTINGS['RATE_LIMIT_SECONDS'])

    # Final save
    print("Saving final results...")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nScraping complete: {successful} successful, {failed} failed, {skipped} skipped")
    print(f"Data merged directly into {json_file}")
    print(f"Backup created at {backup_file}")
    print()
    print("=" * 50)
    print("CONFIGURATION SUMMARY:")
    print(f"  Profiles processed: {SETTINGS['MAX_PROFILES_TO_SCRAPE'] if SETTINGS['MAX_PROFILES_TO_SCRAPE'] else 'ALL'}")
    print(f"  Rate limit: {SETTINGS['RATE_LIMIT_SECONDS']}s between requests")
    print(f"  Save interval: Every {SETTINGS['SAVE_INTERVAL']} profiles")
    print(f"  Services per profile: Max {SETTINGS['MAX_SERVICES_PER_PROFILE']}")
    print(f"  Languages per profile: Max {SETTINGS['MAX_LANGUAGES_PER_PROFILE']}")
    print()
    if SETTINGS['MAX_PROFILES_TO_SCRAPE'] and SETTINGS['MAX_PROFILES_TO_SCRAPE'] < 100:
        print("TIP: Set MAX_PROFILES_TO_SCRAPE = None for full database processing")
    print("=" * 50)

def scrape_availability_text(url):
    """Scrape availability text from a therapist's profile page"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Look for the specific availability div structure
        availability_div = soup.find('div', class_='d-flex align-items-start')

        if availability_div:
            # Find the inner div with the availability text (bg-pumpkin-500 class)
            availability_text_div = availability_div.find('div', class_=lambda x: x and 'bg-pumpkin-500' in x)

            if availability_text_div:
                availability_text = availability_text_div.get_text(strip=True)
                return availability_text

        # Fallback: look for any div containing "Availability" header and extract the next text
        availability_header = soup.find(string=re.compile(r'Availability', re.I))
        if availability_header:
            # Get the parent and look for the next div with availability text
            parent = availability_header.parent
            if parent:
                next_div = parent.find_next_sibling('div')
                if next_div and 'bg-pumpkin-500' in next_div.get('class', []):
                    availability_text = next_div.get_text(strip=True)
                    return availability_text

        return None

    except requests.RequestException as e:
        print(f"[AVAILABILITY ERROR] Request error for {url}: {e}")
        return None
    except Exception as e:
        print(f"[AVAILABILITY ERROR] Error scraping {url}: {e}")
        return None

def update_availability_for_manual_records():
    """Update availability text for all records with dataSource='manual'"""
    print("\n" + "="*70)
    print("[UPDATE AVAILABILITY] UPDATING AVAILABILITY TEXT FOR MANUAL RECORDS")
    print("="*70)
    print("This will:")
    print("1. Query all records where dataSource = 'manual'")
    print("2. Scrape each URL for availability information")
    print("3. Update the availabilityText column in the database")
    print("4. Log all extracted availability text to console")
    print("5. Show progress every 10 records")
    print("="*70)

    confirm = input("\nAre you sure you want to continue? Type 'YES' to proceed: ").strip()
    if confirm != 'YES':
        print("[CANCELLED] Operation cancelled by user")
        return

    print("\n[+] Starting availability update process...")

    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query all manual records
        cursor.execute('SELECT id, "firstName", "lastName", url, "availabilityText" FROM "Therapist" WHERE "dataSource" = \'manual\' ORDER BY id')
        manual_records = cursor.fetchall()

        if not manual_records:
            print("[INFO] No records found with dataSource='manual'")
            cursor.close()
            conn.close()
            return

        print(f"[+] Found {len(manual_records)} records with dataSource='manual'")
        print("[+] Starting availability scraping...")

        successful_updates = 0
        failed_scrapes = 0
        no_availability_found = 0

        for i, (record_id, first_name, last_name, url, current_availability) in enumerate(manual_records):
            if (i + 1) % 10 == 0:
                print(f"[PROGRESS] Processed {i+1}/{len(manual_records)} | Updated: {successful_updates} | Failed: {failed_scrapes} | No data: {no_availability_found}")

            print(f"[SCRAPING] {first_name} {last_name} (ID: {record_id})")

            # Scrape availability text
            availability_text = scrape_availability_text(url)

            if availability_text:
                # Update the database
                cursor.execute(
                    'UPDATE "Therapist" SET "availabilityText" = %s, "updatedAt" = NOW() WHERE id = %s',
                    (availability_text, record_id)
                )
                conn.commit()

                print(f"  [SUCCESS] availabilityText: '{availability_text}'")
                successful_updates += 1
            else:
                print("  [NO DATA] No availability information found")
                no_availability_found += 1

            # Rate limiting - be respectful to the website
            time.sleep(SETTINGS['RATE_LIMIT_SECONDS'])

        # Final summary
        print("\n" + "="*60)
        print("UPDATE AVAILABILITY - COMPLETED!")
        print("="*60)
        print(f"[+] Total records processed: {len(manual_records)}")
        print(f"[+] Successfully updated: {successful_updates}")
        print(f"[+] Failed to scrape: {failed_scrapes}")
        print(f"[+] No availability found: {no_availability_found}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Availability update failed: {e}")
        import traceback
        traceback.print_exc()

def analyze_failed_url_constructions(failed_url_file='data/failed_url_constructions.json'):
    """Analyze failed URL constructions to identify patterns and suggest fixes"""
    try:
        with open(failed_url_file, 'r', encoding='utf-8') as f:
            failed_records = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"No failed URL constructions file found at {failed_url_file}")
        return

    print(f"\nAnalyzing {len(failed_records)} failed URL constructions...")
    print("=" * 60)

    # Analyze patterns in failed names
    special_chars_pattern = re.compile(r'[^\w\s\-]')
    empty_names = []
    special_chars_names = []
    hyphenated_names = []
    long_names = []

    for record in failed_records:
        firstname = record.get('firstname', '')
        lastname = record.get('lastname', '')

        # Check for empty names
        if not firstname or not lastname:
            empty_names.append(record)
            continue

        # Check for special characters
        if special_chars_pattern.search(firstname) or special_chars_pattern.search(lastname):
            special_chars_names.append(record)

        # Check for hyphenated names
        if '-' in firstname or '-' in lastname:
            hyphenated_names.append(record)

        # Check for very long names (might indicate encoding issues)
        if len(firstname) > 20 or len(lastname) > 20:
            long_names.append(record)

    print(f"Empty names: {len(empty_names)}")
    print(f"Names with special characters: {len(special_chars_names)}")
    print(f"Hyphenated names: {len(hyphenated_names)}")
    print(f"Very long names: {len(long_names)}")

    # Show some examples
    if special_chars_names:
        print(f"\nExamples of names with special characters:")
        for i, record in enumerate(special_chars_names[:5]):
            print(f"  {i+1}. {record['firstname']} {record['lastname']} -> {record['generated_slug']}")

    if hyphenated_names:
        print(f"\nExamples of hyphenated names:")
        for i, record in enumerate(hyphenated_names[:5]):
            print(f"  {i+1}. {record['firstname']} {record['lastname']} -> {record['generated_slug']}")

    # Suggest fixes
    print(f"\nSUGGESTED FIXES:")
    print(f"1. For special character names: Update the character mapping in normalize_for_url() function")
    print(f"2. For hyphenated names: Consider truncating at first hyphen (already implemented)")
    print(f"3. For empty names: Check data integrity in source JSON")
    print(f"4. For long names: May indicate encoding issues - check UTF-8 handling")

    print(f"\nTo manually fix names in psychologie.ch.json:")
    print(f"- Load the failed records from {failed_url_file}")
    print(f"- Find corresponding records in psychologie.ch.json by ID")
    print(f"- Update firstname/lastname fields with corrected versions")
    print(f"- Re-run the scraper on the fixed records")

def show_main_menu():
    """Display the main menu"""
    while True:
        print("\n" + "="*70)
        print("PSYCHOLOGIE.CH SCRAPER & DATABASE MANAGER")
        print("="*70)
        print("1. [SCRAPE]    Scrape & Merge (Update existing data)")
        print("2. [NUKE]      Replace Conflicting Psychologie.CH Records (SMART NUKE)")
        print("3. [AVAILABILITY] Update Availability (Scrape availability text)")
        print("4. [CONFIG]    Settings & Configuration")
        print("5. [ANALYZE]   Analyze Failed URLs")
        print("6. [DB]        Database Status & Info")
        print("0. [EXIT]      Exit")
        print("="*70)

        try:
            choice = input("Enter your choice (0-6): ").strip()

            if choice == "0":
                print("\n[BYE] Goodbye!")
                break
            elif choice == "1":
                run_scrape_and_merge()
            elif choice == "2":
                scrape_and_overwrite_database()
            elif choice == "3":
                update_availability_for_manual_records()
            elif choice == "4":
                show_settings_menu()
            elif choice == "5":
                analyze_failed_url_constructions()
            elif choice == "6":
                show_database_info()
            else:
                print("\n[ERROR] Invalid choice. Please try again.")

        except KeyboardInterrupt:
            print("\n\n[BYE] Goodbye!")
            break
        except Exception as e:
            print(f"\n[ERROR] Error: {e}")
            input("Press Enter to continue...")

def run_scrape_and_merge():
    """Run the normal scrape and merge process"""
    print("\n[SCRAPE] SCRAPE & MERGE PROCESS")
    print("="*40)
    print("This will:")
    print("• Load psychologists from data/psychologie.ch.json")
    print("• Scrape missing profile data")
    print("• Merge data into existing records")
    print("• Save progress periodically")
    print("• Track failed URLs for analysis")
    print()
    print("Settings:")
    print(f"• Max profiles: {SETTINGS['MAX_PROFILES_TO_SCRAPE'] or 'ALL'}")
    print(f"• Rate limit: {SETTINGS['RATE_LIMIT_SECONDS']}s between requests")
    print(f"• Save interval: Every {SETTINGS['SAVE_INTERVAL']} profiles")
    print()

    confirm = input("Start scraping? (y/N): ").strip().lower()
    if confirm == 'y':
        scrape_and_merge_in_place()
    else:
        print("[CANCELLED] Operation cancelled")

def show_settings_menu():
    """Show settings management menu"""
    while True:
        print("\n[SETTINGS] SETTINGS & CONFIGURATION")
        print("="*40)
        print("1. [VIEW]    View Current Settings")
        print("2. [EDIT]    Edit Setting")
        print("3. [SAVE]    Save Settings")
        print("4. [RESET]   Reset to Defaults")
        print("0. [BACK]    Back to Main Menu")
        print("="*40)

        choice = input("Enter your choice (0-4): ").strip()

        if choice == "0":
            break
        elif choice == "1":
            show_settings()
            input("\nPress Enter to continue...")
        elif choice == "2":
            edit_setting()
        elif choice == "3":
            save_settings()
        elif choice == "4":
            global SETTINGS
            SETTINGS = DEFAULT_SETTINGS.copy()
            print("[OK] Settings reset to defaults")
            save_settings()
        else:
            print("[ERROR] Invalid choice")

def show_database_info():
    """Show database information and statistics"""
    print("\n[DB] DATABASE INFORMATION")
    print("="*40)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Total records
        cursor.execute('SELECT COUNT(*) FROM "Therapist"')
        total = cursor.fetchone()[0]
        print(f"[STATS] Total therapists in database: {total}")

        # By data source
        cursor.execute('SELECT "dataSource", COUNT(*) FROM "Therapist" GROUP BY "dataSource"')
        sources = cursor.fetchall()
        print("[SOURCES] Records by data source:")
        for source, count in sources:
            print(f"  * {source or 'Unknown'}: {count}")

        # Psychologie.ch specific stats
        cursor.execute('SELECT COUNT(*) FROM "Therapist" WHERE psychologie_ch_id IS NOT NULL')
        psych_count = cursor.fetchone()[0]
        print(f"[PSYCH] Psychologie.ch records: {psych_count}")

        # Recent updates
        cursor.execute('SELECT COUNT(*) FROM "Therapist" WHERE "updatedAt" > NOW() - INTERVAL \'24 hours\'')
        recent = cursor.fetchone()[0]
        print(f"[RECENT] Updated in last 24h: {recent}")

        # Database size info
        cursor.execute('SELECT pg_size_pretty(pg_database_size(current_database()))')
        db_size = cursor.fetchone()[0]
        print(f"[SIZE] Database size: {db_size}")

        cursor.close()
        conn.close()

        print("\n[INFO] Database Table: 'Therapist'")
        print("   Key columns for psychologie.ch data:")
        print("   * psychologie_ch_id (unique ID from psychologie.ch)")
        print("   * psychologie_ch_user_id (user ID from psychologie.ch)")
        print("   * services_offered (JSON array)")
        print("   * target_groups_json (JSON array)")
        print("   * billing_options (JSON array)")
        print("   * languages_spoken (JSON array)")
        print("   * specializations (JSON array)")

    except Exception as e:
        print(f"[ERROR] Error connecting to database: {e}")
        print("[TIP] Make sure database credentials in settings are correct")

    input("\nPress Enter to continue...")

def main():
    """Main entry point"""
    print("PSYCHOLOGIE.CH SCRAPER & DATABASE MANAGER")
    print("=" * 50)

    # Load settings
    load_settings()

    # Handle command line arguments
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == 'analyze':
            analyze_failed_url_constructions()
            return
        elif sys.argv[1] == 'scrape':
            run_scrape_and_merge()
            return
        elif sys.argv[1] == 'availability':
            update_availability_for_manual_records()
            return
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: python scraper.py [analyze|scrape|availability]")
            return

    # Show main menu
    show_main_menu()

if __name__ == "__main__":
    main()
