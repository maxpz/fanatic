url = 'https://api.mailgun.net/v3/{0}/messages'.format(domain)
key = 'key-c962530294e96c209f169f8ad7755e0c'
subject = 'Give yourself the protection you deserve - Enroll in Obamacare Now'
email_from ='Fiorella Insurance <ask@fiorellainsurance.com>'
segundos = 0.05
von_count = 0

with open('dic08_email.csv', 'rb') as uno:
    reader = csv.DictReader(uno)
    for row in reader:
        recipient = row['email']
        html ='''<html xmlns='http://www.w3.org/1999/xhtml'><head><meta http-equiv='Content-Type' content='text/html; cha'''
        #time.sleep(segundos) # tiempo de espera
        request = requests.post(url, auth=('api', key), data={
            'from': email_from,
            'to': recipient,
            'subject': subject,
            'html': html
        })
        #print(name)
        #print(recipient)
        print('Status: {0}'.format(request.status_code))
        print('Body:   {0}'.format(request.text))
        von_count += 1
        print(von_count, request.status_code)