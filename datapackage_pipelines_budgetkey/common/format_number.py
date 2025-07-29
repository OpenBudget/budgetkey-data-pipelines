def format_number(x):
    if x < 1000:
        return  '{:,.2f} ₪'.format(x)
    else:
        return  '{:,.0f} ₪'.format(x)
    
def format_percentage(x):
    if x is None:
        return ''
    elif x < .1:
        return '{:.2f}%'.format(x * 100)
    else:
        return '{:.1f}%'.format(x * 100)