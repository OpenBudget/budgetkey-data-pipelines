digit_lookup = [
    [0,1,2,3,4,5,6,7,8,9],
    [0,2,4,6,8,1,3,5,7,9]
]

def is_valid_israeli_id(id):
    id = str(id) #Make sure id is a string

    if len(id) > 9:
        return False
    if len(id) < 4:
        return False

    if len(id) == 9 and id[0] == '5':
        return False

    sum = 0
    idx = 0
    #traverse the digits of the id from last to first
    for digit in id[::-1]:
        try:
            digit = int(digit)
        except ValueError:
            # If the number is not all digits there will be an error but it also means it is not a valid ID
            return False
        sum += digit_lookup[idx][digit]
        idx = (idx +1) % 2
    return (sum %10) == 0
