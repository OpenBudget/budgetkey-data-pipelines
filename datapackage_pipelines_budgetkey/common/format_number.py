def format_number(x):
    if x < 1000:
        return  '{:,.2f} ₪'.format(x)
    else:
        return  '{:,.0f} ₪'.format(x)