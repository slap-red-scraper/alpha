from setup import Setup
from scrape import Scraper
from log import emit_event, load_historical_metrics
import time

if __name__ == "__main__":
    setup = Setup()
    scraper = Scraper(setup)
    mobile, password = setup.get_credentials()
    url_file = setup.get_url_file()
    downline = setup.downline_enabled()

    with open(url_file) as f:
        urls = [Setup.clean_url(u.strip()) for u in f if u.strip()]
    total_urls = len(urls)
    start = time.time()

    history = load_historical_metrics()
    metrics = {
        'bonuses_old': 0,
        'downlines_old': 0,
        'errors_old': 0,
        'bonuses_total_old': 0,
        'downlines_total_old': 0,
        'errors_total_old': 0,
        'bonuses_change': 0,
        'downlines_change': 0,
        'errors_change': 0,
        'bonuses_new': 0,
        'downlines_new': 0,
        'errors_new': 0,
        'bonuses_total_new': 0,
        'downlines_total_new': 0,
        'errors_total_new': 0
    }

    emit_event("job_start", {"url_count": total_urls})

    for idx, url in enumerate(urls, 1):
        try:
            login_data = setup.login(url, mobile, password)
            if not login_data:
                metrics['errors_new'] += 1
                continue

            if downline:
                count = scraper.fetch_downlines(url, login_data)
                metrics['downlines_new'] += count
                emit_event("downline_fetched", {"count": count})
            else:
                count = scraper.fetch_bonuses(url, login_data)
                metrics['bonuses_new'] += count
                emit_event("bonus_fetched", {"count": count})

        except Exception as e:
            metrics['errors_new'] += 1
            emit_event("exception", {"error": str(e)})

        percent = (idx / total_urls) * 100
        bonuses_change = metrics['bonuses_new'] - metrics['bonuses_total_old']
        downlines_change = metrics['downlines_new'] - metrics['downlines_total_old']
        errors_change = metrics['errors_new'] - metrics['errors_total_old']
        bonuses_total_change = metrics['bonuses_total_new'] - metrics['bonuses_total_old']
        downlines_total_change = metrics['downlines_total_new'] - metrics['downlines_total_old']
        errors_total_change = metrics['errors_total_new'] - metrics['errors_total_old']
        avg_runtime = history['total_runtime'] / history['runs'] if history['runs'] else 0

        print(
            f"[ {idx:03}/{total_urls} | {percent:.2f}% ] [ Avg. Run Time: {avg_runtime:.1f}s ] {url}\n"
            f"[ Bonuses: {metrics['bonuses_old']}/{metrics['bonuses_new']} (+{bonuses_change}) | "
            f"Bonuses Total: {metrics['bonuses_total_old']}/{metrics['bonuses_total_new']} (+{bonuses_total_change}) ] "
            f"[ Downlines: {metrics['downlines_old']}/{metrics['downlines_new']} (+{downlines_change}) | "
            f"Downlines Total: {metrics['downlines_total_old']}/{metrics['downlines_total_new']} (+{downlines_total_change}) ] "
            f"[ Errors: {metrics['errors_old']}/{metrics['errors_new']} (+{errors_change}) | "
            f"Errors Total: {metrics['errors_total_old']}/{metrics['errors_total_new']} (+{errors_total_change}) ]\n"
)

    elapsed = time.time() - start
    emit_event("job_complete", {"duration": elapsed})
