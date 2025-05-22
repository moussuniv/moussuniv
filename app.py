"""
Script Streamlit pour comparer les positions d'une demande (Anfrage) issues d'une base de données
avec les positions d'une offre fournisseur (Angebot) issues d'un fichier CSV.

L'application permet à l'utilisateur de se connecter à une base de données SQL Server,
de spécifier un ID de demande (AnfrageID), et de charger un fichier CSV contenant
les données de l'offre. Elle affiche ensuite les données de la demande et de l'offre,
puis un tableau comparatif mettant en évidence les correspondances, les différences,
les manquants et les extras.
"""
import streamlit as st
import pandas as pd
import pyodbc
import numpy as np

@st.cache_data # Streamlit decorator to cache the output of this function
def connect_db(server: str, database: str, uid: str, pwd: str, anfrage_id: int) -> pd.DataFrame:
    """
    Se connecte à la base de données SQL Server et récupère les données de la table Anfrage_Positionen.
    Utilise st.cache_data pour la mise en cache des résultats.
    Filtre par AnfrageID si celui-ci est fourni et supérieur à 0.

    Args:
        server (str): Nom ou adresse IP du serveur SQL.
        database (str): Nom de la base de données.
        uid (str): Nom d'utilisateur pour la connexion DB.
        pwd (str): Mot de passe pour la connexion DB.
        anfrage_id (int): ID de la demande à filtrer.

    Returns:
        pd.DataFrame: DataFrame contenant les données de la demande (AnfrageID, ArticleNumber, Quantity),
                      ou un DataFrame vide en cas d'erreur.
    """
    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd}"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        query = "SELECT AnfrageID, ArticleNumber, Quantity FROM Anfrage_Positionen"
        params = []
        
        if anfrage_id > 0:
            query += " WHERE AnfrageID = ?"
            params.append(anfrage_id)
            
        df = pd.read_sql(query, conn, params=params if params else None)
        conn.close()
        return df
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        st.error(f"Erreur de connexion à la base de données: {sqlstate}")
        if 'Login failed' in str(ex):
            st.error("Vérifiez vos identifiants de connexion (UID, PWD).")
        elif 'Server not found' in str(ex) or 'TCP/IP error' in str(ex):
            st.error("Vérifiez le nom du serveur et la connectivité réseau.")
        elif 'Database not found' in str(ex):
             st.error(f"La base de données '{database}' n'a pas été trouvée sur le serveur '{server}'.")
        else:
            st.error(f"Détails de l'erreur: {ex}")
        return pd.DataFrame() # Retourne un DataFrame vide en cas d'erreur de connexion
    except Exception as e:
        st.error(f"Une erreur inattendue est survenue lors de la connexion à la base de données: {e}")
        return pd.DataFrame() # Retourne un DataFrame vide pour toute autre exception

@st.cache_data # Streamlit decorator to cache the output of this function
def process_csv(uploaded_file) -> pd.DataFrame:
    """
    Traite le fichier CSV téléversé contenant les données de l'offre (Angebot).
    Regroupe par 'ArticleNumber' et somme les 'Quantity'.
    S'assure que la colonne 'Quantity' est numérique.

    Args:
        uploaded_file: Objet fichier téléversé par Streamlit (st.file_uploader).

    Returns:
        pd.DataFrame: DataFrame contenant les données de l'offre (ArticleNumber, Quantity),
                      ou un DataFrame vide si le fichier est invalide ou en cas d'erreur.
    """
    if uploaded_file is None:
        st.info("Veuillez charger un fichier CSV pour l'offre.")
        return pd.DataFrame()
    try:
        df = pd.read_csv(uploaded_file)
        # Vérification de la présence des colonnes nécessaires
        if 'ArticleNumber' not in df.columns or 'Quantity' not in df.columns:
            st.error("Le fichier CSV doit contenir les colonnes 'ArticleNumber' et 'Quantity'.")
            return pd.DataFrame()
        
        # Conversion de la colonne 'Quantity' en numérique, les erreurs deviennent NaN
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        # Suppression des lignes où 'Quantity' n'a pas pu être converti (NaN)
        df.dropna(subset=['Quantity'], inplace=True)
        
        # Regroupement par ArticleNumber et somme des Quantity
        df_grouped = df.groupby('ArticleNumber', as_index=False)['Quantity'].sum()
        return df_grouped
    except pd.errors.EmptyDataError: # Erreur si le CSV est vide
        st.error("Le fichier CSV est vide.")
        return pd.DataFrame()
    except Exception as e: # Gestion des autres erreurs de lecture ou traitement CSV
        st.error(f"Erreur lors du traitement du fichier CSV: {e}")
        return pd.DataFrame()

def compare_data(df_anfrage: pd.DataFrame, df_angebot: pd.DataFrame) -> pd.DataFrame:
    """
    Compare les données de la demande (Anfrage) et de l'offre (Angebot).
    Fusionne les deux DataFrames sur 'ArticleNumber' et crée une colonne 'Résultat'
    indiquant le statut de chaque article (OK, Quantité différente, Manquant, En plus).

    Args:
        df_anfrage (pd.DataFrame): DataFrame des positions de la demande, doit contenir 'ArticleNumber' et 'Quantity'.
        df_angebot (pd.DataFrame): DataFrame des positions de l'offre, doit contenir 'ArticleNumber' et 'Quantity'.

    Returns:
        pd.DataFrame: DataFrame fusionné avec les colonnes 'ArticleNumber', 'Quantity_Anfrage',
                      'Quantity_Angebot', et 'Résultat'. Retourne un DataFrame vide si les deux entrées sont vides.
    """
    if df_anfrage.empty and df_angebot.empty:
        st.warning("Aucune donnée de demande ou d'offre à comparer.")
        return pd.DataFrame()

    # Ensure columns are named consistently for merging if they come directly from grouping
    df_anfrage = df_anfrage.rename(columns={'Quantity': 'Quantity_Anfrage'})
    df_angebot = df_angebot.rename(columns={'Quantity': 'Quantity_Angebot'})

        return pd.DataFrame() # Retourne un DataFrame vide si la fusion ne donne rien ou si une entrée est vide mais pas l'autre.

    # Renommage des colonnes 'Quantity' pour clarté après la fusion
    df_anfrage = df_anfrage.rename(columns={'Quantity': 'Quantity_Anfrage'})
    df_angebot = df_angebot.rename(columns={'Quantity': 'Quantity_Angebot'})

    # Fusion externe pour conserver tous les ArticleNumber des deux DataFrames
    df_merged = pd.merge(df_anfrage, df_angebot, on='ArticleNumber', how='outer')

    # Remplissage des NaN par 0 pour les quantités. Essentiel pour les articles présents dans un seul des DataFrames.
    df_merged['Quantity_Anfrage'] = df_merged['Quantity_Anfrage'].fillna(0)
    df_merged['Quantity_Angebot'] = df_merged['Quantity_Angebot'].fillna(0)

    # Définition des conditions pour la colonne 'Résultat'
    conditions = [
        (df_merged['Quantity_Anfrage'] > 0) & (df_merged['Quantity_Angebot'] > 0) & (df_merged['Quantity_Anfrage'] == df_merged['Quantity_Angebot']), # Article présent dans les deux, quantité identique
        (df_merged['Quantity_Anfrage'] > 0) & (df_merged['Quantity_Angebot'] > 0) & (df_merged['Quantity_Anfrage'] != df_merged['Quantity_Angebot']), # Article présent dans les deux, quantité différente
        (df_merged['Quantity_Anfrage'] > 0) & (df_merged['Quantity_Angebot'] == 0), # Article présent dans la demande, manquant dans l'offre
        (df_merged['Quantity_Anfrage'] == 0) & (df_merged['Quantity_Angebot'] > 0)  # Article absent de la demande, en plus dans l'offre
    ]
    
    # Définition des résultats correspondants aux conditions
    results = [
        '✅ OK',                          # Statut pour quantité identique
        '⚠️ Quantité différente',         # Statut pour quantité différente
        '❌ Manquant',                    # Statut pour article manquant dans l'offre
        '🆕 En plus (Alternative ?)'     # Statut pour article en plus dans l'offre
    ]
    
    # Création de la colonne 'Résultat' basée sur les conditions
    df_merged['Résultat'] = np.select(conditions, results, default='Erreur') # 'Erreur' si aucune condition n'est remplie (ne devrait pas arriver)
    
    # Réorganisation des colonnes pour une meilleure lisibilité
    cols_order = ['ArticleNumber', 'Quantity_Anfrage', 'Quantity_Angebot', 'Résultat']
    df_merged = df_merged[cols_order]

    return df_merged

def main():
    """
    Fonction principale pour l'application Streamlit Comparateur d'Offres.
    Configure la page, gère les entrées utilisateur dans la barre latérale,
    affiche les données de la demande et de l'offre, et permet la comparaison
    des deux ensembles de données.
    """
    # Configuration de la page Streamlit (doit être la première commande Streamlit)
    st.set_page_config(layout="wide", page_title="Comparateur Offres/Demandes")

    st.title("Comparateur d'Offres Fournisseurs et Demandes Client")

    # --- Barre Latérale (Sidebar) pour les entrées utilisateur ---
    st.sidebar.header("Paramètres de Connexion DB")
    # Champs pour la connexion à la base de données
    db_server = st.sidebar.text_input("Serveur", value="YOUR_SERVER", help="Nom ou adresse IP du serveur SQL.")
    db_database = st.sidebar.text_input("Database", value="YOUR_DATABASE", help="Nom de la base de données.")
    db_uid = st.sidebar.text_input("Utilisateur", value="YOUR_UID", help="Identifiant de connexion à la DB.")
    db_pwd = st.sidebar.text_input("Mot de Passe", value="YOUR_PWD", type="password", help="Mot de passe de connexion à la DB.")

    st.sidebar.header("Filtre Demande (Anfrage)")
    # Champ pour l'ID de la demande
    anfrage_id_filter = st.sidebar.number_input("AnfrageID", min_value=0, step=1, value=0, help="Entrez 0 pour ne pas filtrer ou l'ID spécifique.")

    st.sidebar.header("Offre Fournisseur (CSV)")
    # Champ pour le téléversement du fichier CSV de l'offre
    uploaded_file = st.sidebar.file_uploader("Charger un fichier CSV", type=["csv"], help="Le CSV doit contenir 'ArticleNumber' et 'Quantity'.")

    # --- Initialisation des DataFrames ---
    # DataFrames pour stocker et afficher les données de la demande, de l'offre et du comparatif.
    # Initialisés vides pour éviter les erreurs si les données ne sont pas chargées.
    df_anfrage_display = pd.DataFrame()
    df_angebot_display = pd.DataFrame()
    df_comparison_display = pd.DataFrame() # DataFrame pour le tableau comparatif

    # --- Chargement des Données de la Demande (Anfrage) ---
    # Condition pour tenter de charger les données de la demande :
    # Un ID de demande doit être spécifié et les paramètres DB ne doivent pas être les valeurs par défaut.
    if anfrage_id_filter > 0: # Uniquement si un ID de demande est spécifié
        if db_server and db_server != "YOUR_SERVER" and \
           db_database and db_database != "YOUR_DATABASE" and \
           db_uid and db_uid != "YOUR_UID" and \
           db_pwd: # On vérifie aussi que le mot de passe n'est pas vide (même si YOUR_PWD est une chaine valide)
            # Appel de la fonction de connexion à la DB et récupération des données
            df_anfrage_raw = connect_db(db_server, db_database, db_uid, db_pwd, anfrage_id_filter)
            if not df_anfrage_raw.empty:
                # Regroupement par ArticleNumber et somme des Quantity pour la demande
                df_anfrage_display = df_anfrage_raw.groupby('ArticleNumber', as_index=False)['Quantity'].sum()
        # else: Si les paramètres DB sont ceux par défaut ou manquants, on n'essaie pas de se connecter.
            # Le message d'information dans la section d'affichage de la demande guidera l'utilisateur.

    # --- Chargement des Données de l'Offre (Angebot) ---
    # Condition pour traiter le fichier CSV : Un fichier doit être téléversé.
    if uploaded_file is not None:
        df_angebot_display = process_csv(uploaded_file)

    # --- Affichage Principal (organisé en colonnes) ---
    col1, col2 = st.columns(2) # Crée deux colonnes pour un affichage côte à côte

    with col1: # Colonne de gauche pour les données de la demande
        st.header("Positions de la Demande (Anfrage)")
        if not df_anfrage_display.empty:
            st.dataframe(df_anfrage_display, use_container_width=True)
        elif anfrage_id_filter > 0: # Si un filtre a été appliqué mais aucun résultat (ou échec de connexion)
            st.info("Aucune donnée trouvée pour cette AnfrageID ou erreur de connexion. Vérifiez les paramètres et l'ID.")
        else: # Si aucun filtre n'a été appliqué ou si les paramètres DB sont invalides
            st.info("Veuillez entrer une AnfrageID valide et configurer la connexion DB pour afficher les données de la demande.")

    with col2: # Colonne de droite pour les données de l'offre
        st.header("Positions de l'Offre (Angebot)")
        if not df_angebot_display.empty:
            st.dataframe(df_angebot_display, use_container_width=True)
        elif uploaded_file is not None: # Si un fichier a été chargé mais n'a pas pu être traité ou était vide
            st.info("Le fichier CSV n'a pas pu être traité, est vide, ou ne contient pas les colonnes requises.")
        else: # Si aucun fichier n'a été chargé
            st.info("Veuillez charger un fichier CSV pour afficher les données de l'offre.")

    # --- Section Comparaison ---
    # Le DataFrame df_comparison_display est déjà initialisé plus haut.
    
    # Logique du bouton "Comparer" dans la barre latérale
    if st.sidebar.button("Comparer", key="compare_button", help="Cliquez pour lancer la comparaison après avoir chargé les données."):
        # Condition pour effectuer la comparaison : les deux DataFrames (demande et offre) doivent contenir des données.
        if not df_anfrage_display.empty and not df_angebot_display.empty:
            st.success("Données de la demande et de l'offre chargées. Comparaison en cours...")
            # Appel de la fonction de comparaison. Utilisation de .copy() pour éviter les modifications sur les originaux.
            df_comparison_display = compare_data(df_anfrage_display.copy(), df_angebot_display.copy()) 
            
            # Affichage du tableau comparatif s'il n'est pas vide
            if not df_comparison_display.empty:
                st.header("Tableau Comparatif") # Ré-afficher le header ici pour qu'il n'apparaisse qu'après clic
                st.dataframe(df_comparison_display, use_container_width=True)
                
                # Préparation et affichage du bouton de téléchargement pour le tableau comparatif
                csv_export = df_comparison_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Télécharger le tableau comparatif (CSV)",
                    data=csv_export,
                    file_name="comparaison_offre_demande.csv",
                    mime="text/csv",
                    key="download_csv_button" # Ajout d'une clé au bouton de téléchargement
                )
            else:
                # Cas où la comparaison est effectuée mais ne retourne aucun résultat (ex: aucun ArticleNumber en commun et aucun unique)
                st.header("Tableau Comparatif")
                st.info("La comparaison n'a produit aucun résultat. Vérifiez que les ArticleNumber correspondent entre la demande et l'offre.")
        
        # Messages d'avertissement si l'une des sources de données est manquante lors du clic sur "Comparer"
        elif df_anfrage_display.empty and df_angebot_display.empty:
             st.warning("Veuillez charger les données de la demande (via DB) et de l'offre (via CSV) avant de comparer.")
        elif df_anfrage_display.empty:
            st.warning("Les données de la demande (Anfrage) sont manquantes. Veuillez vérifier la connexion DB et l'AnfrageID.")
        elif df_angebot_display.empty:
            st.warning("Les données de l'offre (Angebot) n'ont pas été chargées ou traitées. Veuillez uploader un fichier CSV valide.")
        else:
            # Fallback générique, ne devrait pas être atteint si les conditions ci-dessus sont logiques
            st.warning("Une erreur inconnue est survenue. Assurez-vous que les données de la demande et de l'offre sont correctement chargées.")
    else:
        # État initial de la section du tableau comparatif (avant que l'utilisateur clique sur "Comparer")
        st.header("Tableau Comparatif") # Toujours afficher le header
        # df_comparison_display est vide à ce stade (ou contient le résultat d'une exécution précédente si Streamlit recharge le script)
        # Mais on ne l'affiche pas encore, on affiche un message d'attente.
        if df_anfrage_display.empty and df_angebot_display.empty:
             st.info("Cliquez sur 'Comparer' dans la barre latérale après avoir configuré la DB, entré une AnfrageID et chargé un fichier CSV.")
        elif df_anfrage_display.empty and not df_angebot_display.empty:
            st.info("En attente des données de la demande (Anfrage). Configurez la DB et entrez une AnfrageID.")
        elif not df_anfrage_display.empty and df_angebot_display.empty:
            st.info("En attente des données de l'offre (Angebot). Chargez un fichier CSV.")
        elif not df_anfrage_display.empty and not df_angebot_display.empty:
             st.info("Données de la demande et de l'offre chargées. Prêt à comparer. Cliquez sur le bouton 'Comparer' dans la barre latérale.")
        else: # Cas par défaut si aucun des états ci-dessus n'est vrai (devrait être rare)
            st.info("Configurez les sources de données et cliquez sur 'Comparer'.")


if __name__ == "__main__":
    main() # Exécute la fonction principale si le script est lancé directement
