# -*- coding: utf-8 -*-
import aiohttp

# local imports
from common import *
from moysklad import MoySklad


async def main():
    ms = MoySklad(MOYSKLAD_TOKEN)
    async with aiohttp.ClientSession() as session:
        # выкачиваем все продукты
        products = await ms.get_products(session)

        # индексируем по href
        products = {x['meta']['href']: x for x in products}

        # выкачиваем все комплекты
        bundles = await ms.get_bundles(session)

        # bundles = [x for x in bundles if x["externalCode"] == 'PSBgndZdguQeXt5eZhJg70']

        for bn in bundles:
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
                print(
                    f'  --- product: {product["name"]}, '
                    f'code: {product["code"]}, '
                    f'ex code: {product["externalCode"]}, '
                    f'buy price: {product["buyPrice"]["value"]}, '
                    f'qty: {qty}'
                )

            price_total = price_fbo + price_buy_total
            if 'attributes' in bn:
                # noinspection SpellCheckingInspection
                attributes = {x['name']: x['value'] for x in bn['attributes'] if 'ртикул' in x['name']}
            else:
                attributes = ''

            print(
                f'bundle: {bn["name"]}, '
                f'code: {bn["code"]}, '
                f'ex code: {bn["externalCode"]}, '
                f'price FBO: {price_fbo}, '
                f'result price: {price_total}, '
                f'attributes: {attributes}'
                f'\n'
            )


if __name__ == '__main__':
    asyncio.run(main())
