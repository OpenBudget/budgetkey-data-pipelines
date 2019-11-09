import dataflows as DF
import time
import logging

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver


def wrapper(wait=False):
    gcd = None
    try:
        gcd = google_chrome_driver(wait=wait)
        return scraper(gcd)
    finally:
        logging.info('Tearing down %r', gcd)
        if gcd:
            gcd.teardown()


def get_chart(driver, charts_wh):
    # Switch to results page & iframe
    driver.switch_to.window(charts_wh)
    frame = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "openDocChildFrame"))
    )
    driver.switch_to.frame(frame)
    chart = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "UIComp_27"))
    )
    return chart


def switch_to_results_page(driver, main_wh, charts_wh):
    results_wh = set(driver.window_handles)
    results_wh.remove(main_wh)
    results_wh.remove(charts_wh)
    results_wh = results_wh.pop()
    driver.switch_to.window(results_wh)
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "ivuFrm_page0ivu0"))
    )
    # Now select the iframe:
    frame = driver.find_element_by_id('ivuFrm_page0ivu0')
    driver.switch_to.frame(frame)
    frame = driver.find_element_by_id('isolatedWorkArea')
    driver.switch_to.frame(frame)
    frame = driver.find_element_by_id('openDocChildFrame')
    driver.switch_to.frame(frame)
    frame = driver.find_element_by_id('webiViewFrame')
    driver.switch_to.frame(frame)
    waitDlg = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "waitDlg"))
    )
    while waitDlg.value_of_css_property('display') != 'none':
        time.sleep(1)


def click_on_export(driver):
    export_button = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "IconImg__dhtmlLib_301"))
    )
    export_button.click()

    data_label = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "label_radioData"))
    )
    data_label.click()

    ok_button = driver.find_element_by_id('OK_BTN_idExportDlg')
    ok_button.click()


def get_results_for_column(driver, column, main_wh, charts_wh):
    column.click()
    time.sleep(10)
    switch_to_results_page(driver, main_wh, charts_wh)
    click_on_export(driver)
    time.sleep(10)
    driver.close()
    driver.switch_to.window(charts_wh)


def scraper(gcd):
    # Open main page
    driver = gcd.driver
    driver.get('http://www.tmichot.gov.il')
    main_wh = driver.current_window_handle
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.ID, "idd1"))
    )
    # Click on 'דוחות'
    frame = driver.find_element_by_id('idd1')
    driver.switch_to.frame(frame)
    el = driver.find_element_by_id('__cell0')
    ActionChains(driver).move_to_element(el)\
                        .move_to_element_with_offset(el, xoffset=10, yoffset=10)\
                        .click()\
                        .perform()
    time.sleep(30)

    # Switch to charts tab
    charts_wh = set(driver.window_handles)
    charts_wh.remove(main_wh)
    charts_wh = charts_wh.pop()
    time.sleep(15)

    # Click on all columns :)
    rects = get_chart(driver, charts_wh).find_elements_by_css_selector('g.v-column .v-datashape')
    for i, rect in enumerate(rects):
        x = i * 30
        driver.execute_script(
            "arguments[0].setAttribute('transform','translate(%d, 0)')" % x, rect
        )
    rects = get_chart(driver, charts_wh).find_elements_by_css_selector('g.v-column .v-datapoint')
    for rect in rects:
        driver.execute_script("arguments[0].setAttribute('height','200')", rect)
    num = len(rects)
    for i in range(num):
        year = 2007 + num - i
        # filename = '/Users/adam/Dropbox (Personal)/hasadna/PublicFiles/supports/%d.csv' % year
        rects = get_chart(driver, charts_wh).find_elements_by_css_selector('rect.v-datapoint')
        get_results_for_column(driver, rects[i], main_wh, charts_wh)
        logging.info('Completed %r, %r', year, gcd.list_downloads())
    time.sleep(20)
    return [gcd.download('https://next.obudget.org/datapackages/' + x) for x in gcd.list_downloads() if x]


def flow(*_):
    return DF.Flow(
        *[
            DF.load(x, encoding='windows-1255', format='csv', name='res%d' % i,
                    infer_strategy=DF.load.INFER_STRINGS,
                    cast_strategy=DF.load.CAST_STRINGS)
            for i, x
            in enumerate(wrapper())
        ],
        DF.update_resource(None, **{'dpp:streaming': True})
    )


if __name__ == '__main__':
    # logging.info(wrapper())
    DF.Flow(
        flow(), DF.printer()
    ).process()
