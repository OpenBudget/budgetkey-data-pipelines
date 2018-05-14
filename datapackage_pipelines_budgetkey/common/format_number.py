def format_number(x):
    if x < 1000:
        return  '{:,.0f} ₪'.format(x)
    else:
        return  '{:,.2f} ₪'.format(x)