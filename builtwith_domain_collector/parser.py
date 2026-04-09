import logging
import re
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


def _clean_domain(raw: str) -> str | None:
    """
    Cleans a raw domain string extracted from the BuiltWith table.

    Examples:
        "  sezane.com  " → "sezane.com"
        "www.fnac.com"   → "fnac.com"
        ""               → None
    """
    if not raw:
        return None
    domain = raw.strip().lower().replace("www.", "")
    # Remove any trailing slashes or paths
    domain = domain.split("/")[0]
    # Validate it looks like a domain
    if not re.match(r"^[a-z0-9\-\.]+\.[a-z]{2,}$", domain):
        return None
    return domain


def _clean_revenue(raw: str) -> str | None:
    """
    Normalizes revenue strings from BuiltWith.
    "$2.6m+"  → "$2.6m+"
    ""         → None
    """
    if not raw or raw.strip() in ("", "-"):
        return None
    return raw.strip()


def _clean_traffic(raw: str) -> str | None:
    """Normalizes traffic tier strings."""
    if not raw or raw.strip() in ("", "-"):
        return None
    return raw.strip()


def parse_table_rows(soup: BeautifulSoup) -> list[dict]:
    """
    Parses the ranking table on a BuiltWith top-sites page.

    BuiltWith table structure:
    <table>
      <thead>
        <tr>
          <th>Rank</th>
          <th></th>       ← favicon column
          <th>Website</th>
          <th>Location</th>
          <th>Sales Revenue</th>
          <th>Tech Spend</th>
          <th>Social</th>
          <th>Employees</th>
          <th>Traffic</th>
          <th></th>       ← action column
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>1</td>
          <td>c</td>          ← first letter of domain (icon)
          <td>cartier.com</td>
          <td>🇫🇷 France</td>
          <td>$2.6m+</td>
          <td>$10000+</td>
          <td>400,000+</td>
          <td></td>
          <td>High</td>
          <td></td>
        </tr>
        ...
      </tbody>
    </table>

    Returns a list of dicts with standardized field names.
    """
    records = []

    table = soup.find("table")
    if not table:
        log.warning("  No table found on page")
        return records

    tbody = table.find("tbody")
    if not tbody:
        log.warning("  No tbody found in table")
        return records

    rows = tbody.find_all("tr")
    log.debug(f"  Found {len(rows)} rows in table")

    for row in rows:
        cells = row.find_all("td")

        # Expect at least 9 cells: rank, icon, domain, location, revenue, spend, social, employees, traffic
        if len(cells) < 9:
            continue

        try:
            rank_raw    = cells[0].get_text(strip=True)
            domain_raw  = cells[2].get_text(strip=True)
            revenue_raw = cells[4].get_text(strip=True)
            spend_raw   = cells[5].get_text(strip=True)
            social_raw  = cells[6].get_text(strip=True)
            traffic_raw = cells[8].get_text(strip=True)

            domain = _clean_domain(domain_raw)
            if not domain:
                continue

            # Parse rank safely
            try:
                rank = int(rank_raw)
            except ValueError:
                rank = None

            records.append({
                "rank":           rank,
                "domain":         domain,
                "sales_revenue":  _clean_revenue(revenue_raw),
                "tech_spend":     _clean_revenue(spend_raw),
                "social_followers": social_raw.strip() if social_raw.strip() else None,
                "traffic_tier":   _clean_traffic(traffic_raw),
                "country":        "FR",
                "source":         "builtwith_top_ecommerce_fr",
            })

        except Exception as e:
            log.debug(f"  Row parse error: {e}")
            continue

    return records


def is_last_page(soup: BeautifulSoup, current_page: int) -> bool:
    """
    Detects if this is the last page of results.

    BuiltWith shows a "Next" pagination link when more pages exist.
    If no Next link is found, we've reached the end.
    """
    # Look for pagination links
    pagination = soup.find_all("a", href=True)
    next_page  = str(current_page + 1)

    for link in pagination:
        href = str(link.get("href") or "")
        if f"p={next_page}" in href:
            return False

    # Also check if the table has any rows at all
    table = soup.find("table")
    if not table:
        return True

    tbody = table.find("tbody")
    if not tbody or not tbody.find_all("tr"):
        return True

    return True   # if no next link found → last page