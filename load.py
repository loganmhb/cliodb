# Emit commands to load /usr/share/dict/words into cliodb.

print('{db:ident word}')

with open('/usr/share/dict/words', 'r') as words:
    for word in words:
        print('{word "%s"}' % word.strip())
