# Floret

<!-- readme-locales:start -->
<p align="center">
  <a href="README.md">English</a> |
  <a href="README.zh-CN.md">简体中文</a> |
  <a href="README.zh-TW.md">繁體中文</a> |
  <a href="README.ja-JP.md">日本語</a> |
  <a href="README.ko-KR.md">한국어</a> |
  <a href="README.de-DE.md">Deutsch</a> |
  <strong>Français</strong> |
  <a href="README.es-ES.md">Español</a> |
  <a href="README.pt-BR.md">Português do Brasil</a> |
  <a href="README.ru-RU.md">Русский</a>
</p>
<!-- readme-locales:end -->

<p align="center">
  <strong>Floret fait tourner l'agent, sans vous enlever votre produit.</strong><br />
  Conversations durables, exécution d'outils, cycle de vie du contexte et faits d'exécution observables pour les applications Go.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/floegence/floret/runtime">
    <img alt="Référence Go" src="https://pkg.go.dev/badge/github.com/floegence/floret/runtime.svg" />
  </a>
  <a href="./LICENSE">
    <img alt="Licence" src="https://img.shields.io/badge/license-MIT-16a34a" />
  </a>
  <img alt="Version de Go" src="https://img.shields.io/badge/Go-1.22-00ADD8?logo=go" />
</p>

![Runtime Floret pour application d'agents IA](okf/assets/readme/floret-agent-app-whiteboard.png)

## Pourquoi Floret ?

Faire appeler un outil par un modèle n'est que le début d'un produit agent. En production, il faut aussi une boucle de modèle robuste, un historique de conversation durable, une exécution d'outils soumise à approbation, une gestion des longs contextes, des opérations récupérables et des faits d'exécution auxquels l'interface peut se fier.

Floret fournit ce runtime sans s'approprier le produit qui l'entoure. Ce n'est ni une UI d'agent, ni un graphe de workflow, ni un framework d'orchestration multi-agent. C'est la couche Go qui sert l'expérience que vous concevez.

Cette séparation est essentielle lorsqu'un produit doit conserver sa personnalité. L'interface, l'identité, les droits, le routage des modèles, les secrets, le modèle de données et les outils métier restent entièrement sous votre contrôle. Floret prend en charge le cycle de vie complexe de l'exécution agent.

### Pour les produits qui ne peuvent pas se contenter d'un modèle imposé

- **Vous gardez la main sur le chemin vers le modèle.** Utilisez la configuration intégrée ou implémentez `runtime.ModelGateway`. Floret pilote les requêtes et leurs continuations ; le transport et les identifiants restent à votre produit.
- **Donnez à chaque Agent un rôle ancré dans votre métier.** `config.AgentProfile.SystemPrompt` ou `config.Config.SystemPrompt` vous permet de définir rôle, ton, scénario métier et règles de fonctionnement, plutôt que de livrer un assistant générique.
- **Les outils et les instructions suivent le travail.** Enregistrez des outils métier stricts dans `tools.Registry`, puis actualisez outils, capacités hébergées, instructions et contexte hôte avec `runtime.ToolSurfaceProvider` à des points sûrs du run.
- **Les conversations deviennent des actifs fiables.** Le runtime Floret gère threads, tours, reprises, forks, sous-threads gérés par le parent et historique sûr pour le provider.
- **La politique d'approbation reste dans le produit.** Floret comprend les effets, ressources et états d'approbation génériques. Votre produit décide qui peut faire quoi, où et pourquoi.
- **L'exécution reste visible.** Connectez événements assainis, pression de contexte, faits de compaction et timelines d'activité neutres à n'importe quelle UI, sans exposer prompts, secrets ou enregistrements internes.
- **Testez le produit, pas les aléas du modèle.** Le Fake Provider rend les flux agent déterministes en local comme en CI.

## Vous gardez la maîtrise

La frontière de Floret est volontairement étroite : Floret possède la mécanique du moteur ; l'application conserve toutes les décisions produit.

| Floret exécute | Votre application décide |
| --- | --- |
| Boucle provider, reprises, continuation des outils et état final | Quand les utilisateurs peuvent lancer, reprendre, interrompre ou annuler un travail |
| Journal de thread, portée de prompt, registre provider et artefacts d'exécution | Utilisateurs, espaces de travail, facturation, métadonnées et rétention produit |
| Validation de schéma, effets génériques, cycle d'approbation et projection des résultats | Autorisation, UI d'approbation, actions métier et textes adressés aux utilisateurs |
| Pression de contexte, cycle de compaction et historique visible par le modèle | Quelles données produit peuvent être transmises au modèle et leur rendu |
| Événements assainis et faits d'activité neutres | Mise en page, flux, contrôles, routage et politique de diagnostic |

Un poste d'exploitation, un environnement de développement, un outil de support et un assistant métier peuvent ainsi partager un runtime fiable sans devenir le même produit.

## Démarrage rapide

Installez les packages stables destinés aux applications en aval :

```bash
go get github.com/floegence/floret/config github.com/floegence/floret/runtime github.com/floegence/floret/tools github.com/floegence/floret/observation
```

```go
store := runtime.NewMemoryStore()
defer store.Close()
var createBinder *runtime.ThreadCreateHostBinder
var turnBinder *runtime.TurnExecutionHostBinder
err := runtime.ConfigureHostCapabilities(store, func(bootstrap *runtime.HostBootstrap) error {
	var err error
	createBinder, err = runtime.NewThreadCreateHostBinder(bootstrap)
	if err != nil { return err }
	turnBinder, err = runtime.NewTurnExecutionHostBinder(bootstrap)
	return err
})
if err != nil { /* handle error */ }

createIntentID := runtime.CreateIntentID("create-thread-1")
threadCreator, err := createBinder.Bind("thread-1", createIntentID)
if err != nil { /* handle error */ }
turnFactory, err := turnBinder.Bind("thread-1")
if err != nil { /* handle error */ }
thread, err := threadCreator.CreateThread(ctx, runtime.CreateThreadRequest{
	ThreadID: "thread-1", CreateIntentID: createIntentID,
})
if err != nil { /* handle error */ }
turnHost, err := turnFactory.NewHost(ctx, runtime.TurnExecutionHostOptions{
	Config: config.Config{
		Provider: config.ProviderFake, Model: "fake-model", FakeResponse: "Hello from Floret.",
		AgentProfile: config.AgentProfile{ID: "support-agent", Name: "Support Agent"},
	},
})
if err != nil { /* handle error */ }

result, err := turnHost.RunTurn(ctx, runtime.RunTurnRequest{
	ThreadID: thread.ID, TurnID: "turn-1", RunID: "run-1",
	Input: runtime.TurnInput{Text: "Welcome a new customer in one sentence."},
})
```

L'exemple complet et directement exécutable se trouve dans le [README anglais](README.md#quick-start). Si votre produit gère le transport du modèle, utilisez une configuration compatible OpenAI ou fournissez un `runtime.ModelGateway`. Pour laisser Floret persister ses propres données d'exécution, utilisez `runtime.OpenSQLiteStore(path)` ; les données produit restent dans votre stockage, reliées par `runtime.ThreadID`.

## Intégration en production

### Faites porter l'intention produit par les prompts

Floret n'impose pas de persona générique. Définissez le rôle initial, le ton, le scénario métier et les règles de fonctionnement de l'Agent avec `config.AgentProfile.SystemPrompt` ou `config.Config.SystemPrompt`. Le prompt est une configuration détenue par l'hôte : un spécialiste du support, un analyste des opérations, un assistant de programmation ou un expert métier peuvent partager le même runtime sans le modifier.

Pour un comportement qui dépend du contexte, faites renvoyer une `runtime.ToolSurface` par `ToolSurfaceProvider`. Elle peut remplacer le prompt système courant en même temps que la surface d'outils, les capacités hébergées et le contexte hôte. Cela convient aux changements de mode produit, d'espace de travail, de droits ou d'étape métier. Floret actualise cette surface avant les requêtes modèle et le dispatch local, de sorte qu'une ancienne décision du modèle ne puisse pas s'exécuter discrètement en dehors de nouvelles instructions ou règles produit.

### N'accordez à la runtime que l'autorité nécessaire

Définissez les actions métier avec `tools.Registry`. Chaque outil possède un JSON Schema strict et peut décrire effets et ressources. Floret valide l'appel, demande une approbation si nécessaire, exécute le handler, enregistre le résultat et le renvoie au modèle. Le handler doit toujours appliquer l'autorisation propre au produit.

### Traitez les identités explicitement

- `ThreadID` identifie la conversation durable.
- `TurnID` identifie un tour visible par l'utilisateur.
- `RunID` identifie une exécution provider précise.
- `PromptScopeID` est la frontière de réutilisation du cache de prompt et du registre provider.

Ces identités sont délibérément distinctes. Pour régler ultérieurement un travail d'outil en attente appartenant à l'hôte, utilisez les `ThreadID`, `TurnID` et `RunID` Floret enregistrés, et non un identifiant d'UI, d'audit ou d'affichage.

### Affichez les faits, pas les détails internes du moteur

Fournissez un `runtime.EventSink` pour recevoir les événements de cycle de vie assainis, et utilisez les DTO `observation` pour la pression de contexte, la compaction et les timelines d'activité. Les événements servent au rendu et au diagnostic de l'hôte ; ils ne sont ni un coffre à secrets ni un substitut à la base produit.

Pour une projection d'affichage durable, utilisez `ThreadTurnProjection` et les API publiques de détail. Ne lisez pas directement les tables de stockage Floret et ne reconstruisez pas l'historique visible du provider dans l'hôte.

### Testez en confiance

Le Fake Provider permet de tester le comportement des outils, les approbations, les reprises, la pression de contexte et les projections de l'UI hôte sans appel à un vrai modèle :

```bash
go test ./...
go run ./cmd/floret-test-ui
```

La console de test locale est destinée aux contributeurs qui inspectent les sessions Fake Provider, événements assainis, scénarios d'outils et sous-threads hébergés. Ce n'est pas une surface d'intégration en aval.

## Surface d'intégration en aval

Les applications en aval ne doivent importer que ces packages publics :

```text
github.com/floegence/floret/config
github.com/floegence/floret/runtime
github.com/floegence/floret/tools
github.com/floegence/floret/observation
```

Tout ce qui se trouve sous `internal/` est un détail d'implémentation. La référence API sur [pkg.go.dev](https://pkg.go.dev/github.com/floegence/floret/runtime) fait foi ; le [bundle de connaissances OKF](okf/index.md) présente l'architecture et le vocabulaire du runtime aux contributeurs.

## Licence

Floret est distribué sous [licence MIT](LICENSE).
