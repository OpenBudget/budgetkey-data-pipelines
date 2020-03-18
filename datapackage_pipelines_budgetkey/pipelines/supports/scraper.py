import dataflows as DF
import time
import logging
import csv

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.remote_connection import ChromeRemoteConnection
ChromeRemoteConnection.set_timeout(300)

from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver


def wrapper(year):
    gcd = None
    try:
        gcd = google_chrome_driver(initial='http://example.com/')
        return scraper(gcd, year)
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
        # EC.presence_of_element_located((By.ID, "UIComp_27"))
        EC.presence_of_element_located((By.ID, "UIComp_0"))
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

    option = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#cbCharDelimiter option:first-child"))
    )
    option.click()

    option = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#cbColSep option:first-child"))
    )
    option.click()

    option = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#cbCharset option:first-child"))
    )
    option.click()

    ok_button = driver.find_element_by_id('OK_BTN_idExportDlg')
    ok_button.click()


def get_results_for_column(driver, column, main_wh, charts_wh):
    column.click()
    time.sleep(10)
    switch_to_results_page(driver, main_wh, charts_wh)
    click_on_export(driver)
    time.sleep(10)
    driver.close()
    # driver.switch_to.window(charts_wh)


def scraper(gcd, selected_year):
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
    time.sleep(10)
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
    groups_selector = 'g.v-m-main g.v-datapoint[combination-column=true]'
    rects_selector = groups_selector + ' rect'
    label_selector = 'g.v-m-main g.v-m-xAxis g.v-m-axisBody text'
    chart = get_chart(driver, charts_wh)
    first_label = chart.find_elements_by_css_selector(label_selector)[0]
    last_year = int(first_label.text)
    print('LAST YEAR', last_year)
    for i in range(100):
        year = last_year - i
        # filename = '/Users/adam/Dropbox (Personal)/hasadna/PublicFiles/supports/%d.csv' % year
        if chart is None:
            chart = get_chart(driver, charts_wh)
        groups = chart.find_elements_by_css_selector(groups_selector)
        rects = chart.find_elements_by_css_selector(rects_selector)
        if i >= len(rects):
            break
        driver.execute_script(
            "arguments[0].setAttribute('transform','translate(%d, 0)')" % (i * 30), groups[i]
        )
        driver.execute_script("arguments[0].setAttribute('height','50')", rects[i])
        if year != selected_year:
            continue
        get_results_for_column(driver, rects[i], main_wh, charts_wh)
        logging.info('Completed %r, %r', year, gcd.list_downloads())
        chart = None
    time.sleep(20)
    return [gcd.download('https://next.obudget.org/datapackages/' + x) for x in gcd.list_downloads() if x]


def flow(parameters, *_):
    return DF.Flow(
        *[
            DF.load(x, format='csv', name='res%d' % i,
                    infer_strategy=DF.load.INFER_STRINGS,
                    cast_strategy=DF.load.CAST_DO_NOTHING)
            for i, x
            in enumerate(wrapper(parameters['year']))
        ],
        DF.update_resource(None, **{'dpp:streaming': True})
    )


if __name__ == '__main__':
    # logging.info(wrapper())
    # DF.Flow(
    #     flow(), DF.printer()
    # ).process()
    from selenium import webdriver
    class b:
        driver = webdriver.Chrome()
    scraper(b())
