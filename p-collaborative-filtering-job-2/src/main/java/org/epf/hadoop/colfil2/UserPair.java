package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPair implements WritableComparable<UserPair> {
    private Text user1;
    private Text user2;

    // Constructeur par défaut
    public UserPair() {
        this.user1 = new Text("");
        this.user2 = new Text("");
    }

    // Constructeur avec paramètres
    public UserPair(String user1, String user2) {
        if (user1.compareTo(user2) <= 0) {
            this.user1 = new Text(user1);
            this.user2 = new Text(user2);
        } else {
            this.user1 = new Text(user2);
            this.user2 = new Text(user1);
        }
    }

    // Méthode 'set' ajoutée pour initialiser ou mettre à jour les utilisateurs
    public void set(String user1, String user2) {
        if (user1.compareTo(user2) <= 0) {
            this.user1.set(user1);
            this.user2.set(user2);
        } else {
            this.user1.set(user2);
            this.user2.set(user1);
        }
    }

    // Implémentation de la méthode 'write' pour la sérialisation
    @Override
    public void write(DataOutput out) throws IOException {
        user1.write(out);
        user2.write(out);
    }

    // Implémentation de la méthode 'readFields' pour la désérialisation
    @Override
    public void readFields(DataInput in) throws IOException {
        user1.readFields(in);
        user2.readFields(in);
    }

    // Implémentation de la méthode 'compareTo' pour comparer deux UserPair
    @Override
    public int compareTo(UserPair o) {
        int cmp = this.user1.compareTo(o.user1);
        if (cmp != 0) {
            return cmp;
        }
        return this.user2.compareTo(o.user2);
    }

    // Méthode toString pour représenter un UserPair sous forme de chaîne
    @Override
    public String toString() {
        return this.user1.toString() + "," + this.user2.toString();
    }

    // Méthode pour obtenir le premier utilisateur
    public String getFirstUser() {
        return user1.toString();
    }

    // Méthode pour obtenir le deuxième utilisateur
    public String getSecondUser() {
        return user2.toString();
    }

    // Implémentation de la méthode equals pour comparer deux objets UserPair
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserPair userPair = (UserPair) o;
        return user1.equals(userPair.user1) && user2.equals(userPair.user2);
    }

    // Implémentation de la méthode hashCode
    @Override
    public int hashCode() {
        return user1.hashCode() * 163 + user2.hashCode();
    }
}