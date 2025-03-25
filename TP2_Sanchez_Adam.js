// Code du TP 2
// Sanchez Adam

const cassandra = require('cassandra-driver');

// Configuration du cluster
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1', // Remplacez par votre nom de datacenter si différent
  keyspace: 'tp2'
});

// Fonction pour afficher tous les utilisateurs
const displayUsers = async () => {
    try {
        const query = 'SELECT username FROM users';
        const result = await client.execute(query);

        console.log('Tous les utilisateurs:');
        result.rows.forEach(user => {
            console.log(user.username);
        });
    } catch (err) {
        console.error('Erreur lors de la récupération des utilisateurs:', err);
    }
};
  
// Fonction pour afficher les utilisateurs suivis par un utilisateur spécifique
const displayFollowing = async (username) => {
    try {
        const query = 'SELECT followed FROM following WHERE username = ?';
        const result = await client.execute(query, [username], { prepare: true });

        if (result.rows.length > 0) {
            console.log(`${username} suit:`);
            result.rows.forEach(row => {
                console.log(row.followed);
            });
        } else {
            console.log(`${username} ne suit personne.`);
        }
    } catch (err) {
        console.error('Erreur lors de la récupération des utilisateurs suivis:', err);
    }
};

// Fonction pour afficher les utilisateurs qui suivent un utilisateur spécifique
const displayFollowers = async (username) => {
    try {
    const query = 'SELECT following FROM followers WHERE username = ?';
    const result = await client.execute(query, [username], { prepare: true });

    if (result.rows.length > 0) {
        console.log(`${username} est suivi par:`);
        result.rows.forEach(row => {
            console.log(row.follower);
        });
    } else {
        console.log(`${username} n'a pas de followers.`);
    }
    } catch (err) {
    console.error('Erreur lors de la récupération des followers:', err);
    }
};
  
// Fonction pour afficher tous les shouts
const displayShouts = async () => {
    try {
        const query = 'SELECT body FROM shouts';
        const result = await client.execute(query);

        console.log('Tous les shouts:');
        result.rows.forEach(shout => console.log(shout.body));
    } catch (err) {
    console.error('Erreur lors de la récupération des shouts:', err);
    }
};
  
// Fonction pour afficher les shouts d'un utilisateur spécifique
const displayUserShouts = async (username) => {
    try {
        const query = 'SELECT body FROM shouts WHERE username = ?';
        const result = await client.execute(query, [username], { prepare: true });

        if (result.rows.length > 0) {
            console.log(`Shouts de ${username}:`);
            result.rows.forEach(shout => console.log(shout.body));
        } else {
            console.log(`${username} n'a pas de shouts.`);
        }
    } catch (err) {
        console.error('Erreur lors de la récupération des shouts:', err);
    }
};

async function run() {
  try {
    // Connectez-vous au cluster
    await client.connect();
    console.log('Connected to Cassandra');

    // Requêtes
    console.log('-------------------------------');
    await displayUsers();
    console.log('-------------------------------');
    await displayFollowing('homer');
    console.log('-------------------------------');
    await displayFollowers('homer');
    console.log('-------------------------------');
    await displayShouts();
    console.log('-------------------------------');
    await displayUserShouts('homer');


  } catch (err) {
    console.error('There was an error', err);
  } finally {
    // Fermer la connexion
    await client.shutdown();
  }
}

run();
