# -*- coding: utf-8 -*-
import aiohttp
import xlsxwriter
from datetime import datetime
from tqdm import tqdm

# local imports
from common import *
from moysklad import MoySklad


async def main():
    ms = MoySklad(MOYSKLAD_TOKEN)
    result = []
    async with aiohttp.ClientSession() as session:
        # выкачиваем все продукты
        log.info('fetching products')
        products = await ms.get_products(session)
        log.info(f'products fetched: {len(products)}')

        # индексируем по href
        products = {x['meta']['href']: x for x in products}

        # выкачиваем все комплекты
        log.info('fetching bundles')
        bundles = await ms.get_bundles(session)
        log.info(f'bundles fetched: {len(bundles)}')

        log.info('fetching bundle components')
        for bn in tqdm(bundles):
            price_fbo = [x["value"] for x in bn["salePrices"] if x["priceType"]["name"] == 'Цена FBO'][0]
            components_meta = bn['components']['meta']
            assert components_meta['size'] <= components_meta['limit']

            # выкачиваем список компонентов комплекта
            rows = await ms.get_bundle_component_rows(session, components_meta['href'])

            price_buy_total = 0.0
            for row in rows:
                qty = row['quantity']
                product_meta = row['assortment']['meta']
                assert product_meta['type'] == 'product'
                product_href = product_meta['href']

                if product_href in products:
                    product = products[product_href]
                else:
                    # продукта почему-то нет в тех, что выкачаны, выкачиваем отдельно
                    product = await ms.get_product(session, product_href)

                price_buy = product['buyPrice']['value']
                price_buy_total += price_buy * qty
                # print(
                #     f'  --- product: {product["name"]}, '
                #     f 'code: {product["code"]}, '
                #     f 'ex code: {product["externalCode"]}, '
                #     f 'buy price: {product["buyPrice"]["value"]}, '
                #     f 'qty: {qty}'
                # )

            price_total = price_fbo + price_buy_total
            code_ozon, code_wb, code_ya = '', '', ''
            if 'attributes' in bn:
                for attr in bn['attributes']:
                    attr_name = attr['name']
                    if attr_name == 'Артикул поставщика ОЗОН':
                        code_ozon = attr['value']
                    elif attr_name == 'артикул поставщика WB':
                        code_wb = attr['value']
                    elif attr_name == 'Артикул поставщика Я.Маркет':
                        code_ya = attr['value']
                    elif 'артикул' in attr_name.lower():
                        log.warning(f'обнаружен артикул неопознанного поставщика: {attr_name}')

            result.append({
                'name': bn['name'],
                'code': bn['code'],
                'ex_code': bn['externalCode'],
                'price_fbo': price_fbo / 100,
                'buy_price': price_total / 100,
                'code_wb': code_wb,
                'code_ozon': code_ozon,
                'code_ya': code_ya,
                'product_link': bn['meta']['uuidHref']
            })
            # print('.', end='', flush=True)

    # print('', flush=True)
    log.info('write xlsx file')
    output_filename = f'{FILENAME_BASE_XLS_OUTPUT}-{datetime.now().astimezone():%Y%m%d%H%M}.xlsx'

    workbook = xlsxwriter.Workbook(output_filename)
    worksheet = workbook.add_worksheet()
    worksheet.set_column('A:A', 80)
    worksheet.set_column('B:B', 19)
    worksheet.set_column('C:C', 25)
    worksheet.set_column('D:D', 8)
    worksheet.set_column('E:E', 8)
    worksheet.set_column('F:F', 24)
    worksheet.set_column('G:G', 24)
    worksheet.set_column('H:H', 24)
    worksheet.set_column('I:I', 72)

    cell_fmt_header = workbook.add_format()
    cell_fmt_header.set_bold()

    cell_fmt_price = workbook.add_format()
    cell_fmt_price.set_num_format('#,##0.00')

    cell_fmt_text = workbook.add_format()
    cell_fmt_text.set_num_format('@')  # text format

    row, col = 0, 0
    worksheet.write(row, col + 0, 'Название комплекта', cell_fmt_header)
    worksheet.write(row, col + 1, 'Код', cell_fmt_header)
    worksheet.write(row, col + 2, 'Внешний код', cell_fmt_header)
    worksheet.write(row, col + 3, 'FBO', cell_fmt_header)
    worksheet.write(row, col + 4, 'Зак. цена', cell_fmt_header)
    worksheet.write(row, col + 5, 'Артикул WB', cell_fmt_header)
    worksheet.write(row, col + 6, 'Артикул ОЗОН', cell_fmt_header)
    worksheet.write(row, col + 7, 'Артикул Я.Маркет', cell_fmt_header)
    worksheet.write(row, col + 8, 'Страница редактирования комплекта', cell_fmt_header)
    row += 1
    for item in result:
        worksheet.write(row, col + 0, item['name'], cell_fmt_text)
        worksheet.write(row, col + 1, item['code'], cell_fmt_text)
        worksheet.write(row, col + 2, item['ex_code'], cell_fmt_text)
        worksheet.write(row, col + 3, item['price_fbo'], cell_fmt_price)
        worksheet.write(row, col + 4, item['buy_price'], cell_fmt_price)
        worksheet.write(row, col + 5, item['code_wb'], cell_fmt_text)
        worksheet.write(row, col + 6, item['code_ozon'], cell_fmt_text)
        worksheet.write(row, col + 7, item['code_ya'], cell_fmt_text)
        worksheet.write(row, col + 8, item['product_link'])
        row += 1

    workbook.close()
    log.info(f'saved to file {output_filename}')


if __name__ == '__main__':
    asyncio.run(main())
