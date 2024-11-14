# -*- coding: utf-8 -*-
from common import *

URL_MOYSKLAD_API_BASE = 'https://api.moysklad.ru/api/remap/1.2'
URL_MOYSKLAD_API_PRODUCT = f'{URL_MOYSKLAD_API_BASE}/entity/product'
URL_MOYSKLAD_API_BUNDLE = f'{URL_MOYSKLAD_API_BASE}/entity/bundle'
URL_MOYSKLAD_API_ORGANIZATION = f'{URL_MOYSKLAD_API_BASE}/entity/organization'
URL_MOYSKLAD_API_PRICETYPE = f'{URL_MOYSKLAD_API_BASE}/context/companysettings/pricetype'
URL_MOYSKLAD_API_ASSORTMENT_CUSTOMTEMPLATE = f'{URL_MOYSKLAD_API_BASE}/entity/assortment/metadata/customtemplate'


class MoySklad:
    def __init__(self, token):
        self.token = token
        self._headers = {'Authorization': f'Bearer {token}'}

    async def search_products(self, session, search):
        params = {
            'search': search,
        }
        result = await http_request(session.get, headers=self._headers, url=URL_MOYSKLAD_API_PRODUCT, params=params)
        products_total = result['meta']['size']
        products = result['rows']
        assert len(products) == products_total, 'got incorrect number of products'
        return products

    async def get_bundles_by_code(self, session, code):
        params = {
            'filter': f'code~{code}',
        }
        result = await http_request(session.get, headers=self._headers, url=URL_MOYSKLAD_API_BUNDLE, params=params)
        bundles_total = result['meta']['size']
        bundles = result['rows']
        assert len(bundles) == bundles_total, 'got incorrect number of bundles'
        return bundles

    async def get_all_entities(self, session, url: str):
        offset = 0
        all_entities = []
        while True:
            params = {
                'offset': offset,
                'limit': ENTITIES_TAKE_MAX
            }
            result = await http_request(session.get, headers=self._headers, url=url, params=params)
            entities = result['rows']
            size = result['meta']['size']
            offset_got = result['meta']['offset']
            if offset != offset_got:
                raise RuntimeError(f'got offset: {offset_got}, expected: {offset}')
            expected_num = min(ENTITIES_TAKE_MAX, size - offset if size > offset else 0)
            if len(entities) != expected_num:
                raise RuntimeError(f'entities fetched: {len(entities)}, expected: {expected_num}')
            if not entities:
                break
            all_entities += entities
            offset += expected_num
        return all_entities

    async def get_bundles(self, session):
        return await self.get_all_entities(session, URL_MOYSKLAD_API_BUNDLE)

    async def get_products(self, session):
        return await self.get_all_entities(session, URL_MOYSKLAD_API_PRODUCT)

    async def get_bundle_component_rows(self, session, href):
        result = await http_request(session.get, headers=self._headers, url=href)
        rows = result['rows']
        assert result['meta']['size'] == len(rows)
        return rows

    async def get_product(self, session, href):
        result = await http_request(session.get, headers=self._headers, url=href)
        return result

    async def get_product_href(self, session, barcode):
        products = await self.search_products(session, search=barcode)
        products = [x for x in products if x['code'] == barcode]
        if not products:
            return None
        assert len(products) == 1, f'there are several products with barcode "{barcode}"'
        return products[0]['meta']['href']

    async def get_bundle_href(self, session, barcode):
        bundles = await self.get_bundles_by_code(session, code=barcode)
        bundles = [x for x in bundles if x['code'] == barcode]
        if not bundles:
            return None
        assert len(bundles) == 1, f'there are several bundles with barcode "{barcode}"'
        return bundles[0]['meta']['href']

    async def get_product_or_bundle_label(self, session, org_href, pricetype_href, template_href, barcode):
        params = {
            "organization": {
                "meta": {
                    "href": org_href,
                    "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/organization/metadata",
                    "type": "organization",
                    "mediaType": "application/json"
                }
            },
            "count": 1,
            "salePrice": {
                "priceType": {
                    "meta": {
                        "href": pricetype_href,
                        "type": "pricetype",
                        "mediaType": "application/json"
                    }
                }
            },
            "template": {
                "meta": {
                    "href": template_href,
                    "type": "customtemplate",
                    "mediaType": "application/json"
                }
            }
        }
        product_href = await self.get_product_href(session, barcode)
        if not product_href:
            product_href = await self.get_bundle_href(session, barcode)
            if not product_href:
                return None
        result = await http_request(
            session.post, headers=self._headers, url=f'{product_href}/export', json=params, result_binary=True)
        return result

    async def get_organization_href(self, session, name):
        result = await http_request(session.get, headers=self._headers, url=URL_MOYSKLAD_API_ORGANIZATION)
        rows = [x for x in result['rows'] if x['name'] == name]
        assert rows, f'organization with name "{name}" not found'
        assert len(rows) == 1, f'there are several organizations with name "{name}"'
        return rows[0]['meta']['href']

    async def get_customtemplate_href(self, session, name):
        result = await http_request(session.get, headers=self._headers, url=URL_MOYSKLAD_API_ASSORTMENT_CUSTOMTEMPLATE)
        rows = [x for x in result['rows'] if x['name'] == name]
        assert len(rows) <= 1, f'there are several custom templates with name "{name}"'
        assert rows, f'custom template with name "{name}" not found'
        return rows[0]['meta']['href']

    async def get_pricetype_href(self, session, name):
        result = await http_request(session.get, headers=self._headers, url=URL_MOYSKLAD_API_PRICETYPE)
        rows = [x for x in result if x['name'] == name]
        assert rows, f'pricetype with name "{name}" not found'
        assert len(rows) == 1, f'there are several price types with name "{name}"'
        return rows[0]['meta']['href']
