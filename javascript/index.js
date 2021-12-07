const sqlite3 = require('sqlite3')
const open = require('sqlite').open
const fs = require('fs')

const filename = 'contacts.sqlite3'
const numContacts = 100000 // TODO: read from process.argv

const shouldMigrate = !fs.existsSync(filename)

/**
 * Generate `numContacts` contacts,
 * one at a time
 *
 */
function * generateContacts (numContacts) {
  let i = 1
  while (i <= numContacts) {
    yield [i, `name-${i}`, `email-${i}@domain.tld`]
    i++
  }
}

const migrate = async (db) => {
  console.log('Migrating db ...')
  await db.exec(`
        CREATE TABLE contacts(
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL
         )
     `);
  await db.exec('CREATE UNIQUE INDEX index_contacts_email ON contacts(email);');
  console.log('Done migrating db')
}

const insertContacts = async (db) => {
  console.log('Inserting contacts ...');
  const contacts = generateContacts(numContacts);
  let contactsLeft = numContacts;

  while(contactsLeft > 0) {
    const contactsInfos = [];

    let contactsToAdd = 10000;
    if (contactsLeft < contactsToAdd) contactsToAdd = contactsLeft;

    let query = `INSERT INTO contacts VALUES `;
    for (let j = 0; j < contactsToAdd; j++) {
      contactsInfos.push(...contacts.next().value);
      query += `(?, ?, ?)`;
      if (j + 1  !== contactsToAdd) {
        query += ', ';
      }
    }
    query += ';';
    await db.run(query, contactsInfos);

    contactsLeft -= contactsToAdd;

    console.log('contacts Left = ' + contactsLeft);
  }
}

const queryContact = async (db) => {
  const start = Date.now()
  const res = await db.get('SELECT name FROM contacts WHERE email = ?', [`email-${numContacts}@domain.tld`])
  if (!res || !res.name) {
    console.error('Contact not found')
    process.exit(1)
  }
  const end = Date.now()
  const elapsed = (end - start)
  console.log(`Query took ${elapsed} ms`)
}

(async () => {
  const db = await open({
    filename,
    driver: sqlite3.Database
  })
  if (shouldMigrate) {
    await migrate(db)
  }
  await insertContacts(db)
  await queryContact(db)
  await db.close()
})()
