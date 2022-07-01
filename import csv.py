import csv
import json
import time
from tkinter import N

import click
import shopify

from config import CREDENTIALS
from helpers import iter_graphql_response
from queries import GET_PRODUCTS_QUERY, GET_COLLECTIONS_QUERY

API_VERSION = '2022-04'

VALID_CUSTOM_PRODUCT_TYPES = (
    'Button Downs',
    'Polos',
)

collectionRulesConditions {
    
}

# TODO: Add option to iterate over all products or just status=active products

def iter_products(ctx):
    total_queries = 0
    total_query_cost = 0

    page = 1
    has_next_page = True
    cursor = None

    while has_next_page:
        vars = {
            'count': 25
        }
        if cursor:
            vars['cursor'] = cursor
        result = json.loads(shopify.GraphQL().execute(GET_PRODUCTS_QUERY, variables=vars))

        has_next_page = result['data']['products']['pageInfo']['hasNextPage']

        for product in result['data']['products']['edges']:
            cursor = product['cursor']
            yield product
        
        if ctx.obj['DEBUG']:
            click.echo('Completed page {}'.format(page))

        page += 1

        # Rate limit ourselves based on actual query cost
        total_queries += 1
        total_query_cost += int(result['extensions']['cost']['actualQueryCost'])
        avg_query_cost = 1.0 * total_query_cost / total_queries

        # if the next query will put us below the halfway point of available points, then pause
        if float(result['extensions']['cost']['throttleStatus']['currentlyAvailable']) <= float(result['extensions']['cost']['throttleStatus']['maximumAvailable']) / 2:
            time.sleep(avg_query_cost / float(result['extensions']['cost']['throttleStatus']['restoreRate']))

@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug

    shop_url = "https://{}/admin/api/{}".format(CREDENTIALS['shop_url'], API_VERSION)
    session = shopify.Session(shop_url, API_VERSION, CREDENTIALS['access_token'])
    shopify.ShopifyResource.activate_session(session)

    @ctx.call_on_close
    def close_shopify_client():
        click.echo('Closing shopify session.')
        shopify.ShopifyResource.clear_session()

@cli.command()
@click.argument('output', type=click.File('w'))
@click.pass_context
def check_standard_product_types(ctx, output):
    writer = csv.DictWriter(output, fieldnames=['id', 'handle', 'title', 'vendor', 'status', 'standardizedProductType', 'issue'])
    writer.writeheader()

    for product in iter_products(ctx):
        standardizedProductType = None
        if product['node']['standardizedProductType']:
            standardizedProductType = product['node']['standardizedProductType']['productTaxonomyNode']['fullName']
        
        if not standardizedProductType:
            issue = 'standardizedProductType is blank'
        elif not product['node']['standardizedProductType']['productTaxonomyNode']['isLeaf']:
            issue = 'standardizedProductType is not a leaf node'
        else:
            issue = ''
        
        if issue:
            if ctx.obj['DEBUG']:
                click.echo('Found an issue with {}'.format(product['node']['handle']))
            writer.writerow({
                'id': product['node']['id'],
                'handle': product['node']['handle'],
                'title': product['node']['title'],
                'vendor': product['node']['vendor'],
                'status': product['node']['status'],
                'standardizedProductType': standardizedProductType,
                'issue': issue,
            })

@cli.command()
@click.argument('output', type=click.File('w'))
@click.pass_context
def check_custom_product_types(ctx, output):
    writer = csv.DictWriter(output, fieldnames=['id', 'handle', 'title', 'vendor', 'status', 'customProductType', 'issue'])
    writer.writeheader()

    for product in iter_products(ctx):
        if not any(product['node']['customProductType'] == valid_product_type for valid_product_type in VALID_CUSTOM_PRODUCT_TYPES):
            issue = 'customProductType is not a valid value'
        else:
            issue = ''
        
        if issue:
            if ctx.obj['DEBUG']:
                click.echo('Found an issue with {}'.format(product['node']['handle']))
            writer.writerow({
                'id': product['node']['id'],
                'handle': product['node']['handle'],
                'title': product['node']['title'],
                'vendor': product['node']['vendor'],
                'status': product['node']['status'],
                'customProductType': product['node']['customProductType'],
                'issue': issue,
            })


@cli.command()
@click.argument('output', type=click.File('w'))
@click.pass_context
def check_product_data_rules(ctx, output):
    # This is an alternative way to organize rules where all rules for a particular data type (in this case products) are run at once.
    # This makes a lot of sense because we only have to iterate through the API once instead of once per rule.
    # The output may be slightly chaotic once all the rules are defined though.
    writer = csv.DictWriter(output, fieldnames=['id', 'handle', 'title', 'vendor', 'status', 'standardizedProductType', 'customProductType', 'standardizedProductTypeIssue', 'customProductTypeIssue'])
    writer.writeheader()

    for product in iter_products(ctx):
        issue = False

        standardizedProductType = None
        if product['node']['standardizedProductType']:
            standardizedProductType = product['node']['standardizedProductType']['productTaxonomyNode']['fullName']
        
        if not standardizedProductType:
            standardizedProductTypeIssue = 'standardizedProductType is blank'
            issue=True
        elif not product['node']['standardizedProductType']['productTaxonomyNode']['isLeaf']:
            standardizedProductTypeIssue = 'standardizedProductType is not a leaf node'
            issue=True
        else:
            standardizedProductTypeIssue = ''
        
        if not any(product['node']['customProductType'] == valid_product_type for valid_product_type in VALID_CUSTOM_PRODUCT_TYPES):
            customProductTypeIssue = 'customProductType not in valid values'
            issue=True
        
        if issue:
            if ctx.obj['DEBUG']:
                click.echo('Found an issue with {}'.format(product['node']['handle']))
            writer.writerow({
                'id': product['node']['id'],
                'handle': product['node']['handle'],
                'title': product['node']['title'],
                'vendor': product['node']['vendor'],
                'status': product['node']['status'],
                'standardizedProductType': standardizedProductType,
                'standardizedProductTypeIssue': standardizedProductTypeIssue,
                'customProductTypeIssue': customProductTypeIssue,
            })

@cli.command()
@click.argument('output', type=click.File('w'))
@click.pass_context
def list_collections(ctx, output):
    click.echo('Listing collections.')

    writer = csv.DictWriter(output, fieldnames=['id', 'handle', 'title', 'productsCount', 'updatedAt'])
    writer.writeheader()

    for collection in iter_graphql_response(ctx, GET_COLLECTIONS_QUERY, 'collections'):
        writer.writerow({
            'id': collection['node']['id'],
            'handle': collection['node']['handle'],
            'title': collection['node']['title'],
            'productsCount': collection['node']['productsCount'],
            'updatedAt': collection['node']['updatedAt'],
        })

if __name__ == '__main__':
    cli(obj={})
