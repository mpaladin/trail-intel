from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from trailintel.participants import (
    ENDU_HEADERS,
    GENERIC_BROWSER_HEADERS,
    RACERESULT_HEADERS,
    _extract_names_from_html,
    dedupe_names,
    fetch_participants_from_url,
    looks_like_name,
)


class ParticipantParsingTests(unittest.TestCase):
    def test_looks_like_name(self) -> None:
        self.assertTrue(looks_like_name("Kilian Jornet"))
        self.assertFalse(looks_like_name("Rank"))
        self.assertFalse(looks_like_name("1234"))

    def test_dedupe_names_case_insensitive(self) -> None:
        names = dedupe_names(["Jim Walmsley", "jim walmsley", "Kilian Jornet"])
        self.assertEqual(names, ["Jim Walmsley", "Kilian Jornet"])

    def test_extract_names_from_html_table(self) -> None:
        html = """
        <table>
          <tr><th>Bib</th><th>Name</th></tr>
          <tr><td>12</td><td>Kilian Jornet</td></tr>
          <tr><td>22</td><td>Jim Walmsley</td></tr>
        </table>
        """
        names = _extract_names_from_html(html)
        self.assertIn("Kilian Jornet", names)
        self.assertIn("Jim Walmsley", names)

    @patch("trailintel.participants.requests.get")
    def test_fetch_yaka_participants_with_competition_filter(
        self, mock_get: Mock
    ) -> None:
        edition_payload = {
            "_id": "edition-1",
            "competitions": [
                {
                    "_id": "comp-40",
                    "name": [{"translation": "Le 40 km", "language": "fr"}],
                },
                {
                    "_id": "comp-15",
                    "name": [{"translation": "Le 15 km", "language": "fr"}],
                },
            ],
        }
        registrations_payload = [
            {"firstname": "Alice", "lastname": "Martin", "competition": "comp-40"},
            {"firstname": "Bob", "lastname": "Trail", "competition": "comp-15"},
            {"firstname": "Alice", "lastname": "Martin", "competition": "comp-40"},
        ]

        edition_response = Mock()
        edition_response.raise_for_status.return_value = None
        edition_response.json.return_value = edition_payload

        registrations_response = Mock()
        registrations_response.raise_for_status.return_value = None
        registrations_response.json.return_value = registrations_payload

        mock_get.side_effect = [edition_response, registrations_response]

        names = fetch_participants_from_url(
            "https://in.yaka-inscription.com/trail-du-sanglier-2026?currentPage=select-competition",
            competition_name="40 km",
        )

        self.assertEqual(names, ["Alice Martin"])
        self.assertEqual(
            mock_get.call_args_list[0].args[0],
            "https://front-api.yaka-inscription.com/edition/url/trail-du-sanglier-2026",
        )
        self.assertEqual(
            mock_get.call_args_list[1].args[0],
            "https://front-api.yaka-inscription.com/registrations/edition-1/_search/%7B%7D",
        )

    @patch("trailintel.participants.requests.get")
    def test_fetch_yaka_raises_on_unknown_competition(self, mock_get: Mock) -> None:
        edition_payload = {
            "_id": "edition-1",
            "competitions": [
                {
                    "_id": "comp-40",
                    "name": [{"translation": "Le 40 km", "language": "fr"}],
                }
            ],
        }
        edition_response = Mock()
        edition_response.raise_for_status.return_value = None
        edition_response.json.return_value = edition_payload
        mock_get.return_value = edition_response

        with self.assertRaises(ValueError):
            fetch_participants_from_url(
                "https://in.yaka-inscription.com/trail-du-sanglier-2026?currentPage=select-competition",
                competition_name="28 km",
            )

    @patch("trailintel.participants.requests.get")
    def test_fetch_njuko_participants_with_competition_filter(
        self, mock_get: Mock
    ) -> None:
        edition_payload = {
            "_id": "edition-njuko-1",
            "competitions": [
                {
                    "_id": "comp-42",
                    "name": [
                        {"translation": "Maratrail des hauts du lac - 42km - 1900m D+"}
                    ],
                },
                {
                    "_id": "comp-23",
                    "name": [{"translation": "Trail perché - 23km - 800m D+"}],
                },
            ],
        }
        registrations_payload = [
            {"firstname": "Tiphaine", "lastname": "Delente", "competition": "comp-42"},
            {"firstname": "Magali", "lastname": "Gaurand", "competition": "comp-42"},
            {"firstname": "Laure", "lastname": "Winsback", "competition": "comp-23"},
            {"firstname": "Tiphaine", "lastname": "Delente", "competition": "comp-42"},
        ]

        edition_response = Mock()
        edition_response.raise_for_status.return_value = None
        edition_response.json.return_value = edition_payload

        registrations_response = Mock()
        registrations_response.raise_for_status.return_value = None
        registrations_response.json.return_value = registrations_payload

        mock_get.side_effect = [edition_response, registrations_response]

        names = fetch_participants_from_url(
            "https://in.njuko.com/entrelacs-run-and-trail-2026?currentPage=select-competition",
            competition_name="Maratrail des hauts du lac - 42km",
        )

        self.assertEqual(names, ["Tiphaine Delente", "Magali Gaurand"])
        self.assertEqual(
            mock_get.call_args_list[0].args[0],
            "https://front-api.njuko.com/edition/url/entrelacs-run-and-trail-2026",
        )
        self.assertEqual(
            mock_get.call_args_list[1].args[0],
            "https://front-api.njuko.com/registrations/edition-njuko-1/_search/%7B%7D",
        )

    @patch("trailintel.participants.requests.get")
    def test_fetch_njuko_raises_on_unknown_competition(self, mock_get: Mock) -> None:
        edition_payload = {
            "_id": "edition-njuko-1",
            "competitions": [
                {
                    "_id": "comp-42",
                    "name": [
                        {"translation": "Maratrail des hauts du lac - 42km - 1900m D+"}
                    ],
                }
            ],
        }
        edition_response = Mock()
        edition_response.raise_for_status.return_value = None
        edition_response.json.return_value = edition_payload
        mock_get.return_value = edition_response

        with self.assertRaises(ValueError):
            fetch_participants_from_url(
                "https://in.njuko.com/entrelacs-run-and-trail-2026?currentPage=select-competition",
                competition_name="Non existing distance",
            )

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_participants_data_url(self, mock_get: Mock) -> None:
        payload = {
            "DataFields": ["BIB", "ID", "ucase([LFNAME])", "NATION.IOCNAME"],
            "data": {
                "#1_72K": {
                    "#1_F": [["1297", "2300", "AGUILAR RIOS, EMMA", "ESP"]],
                    "#2_M": [["1226", "1610", "AGOSTINI, ANDREA", "ITA"]],
                }
            },
        }

        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://my1.raceresult.com/364696/RRPublish/data/list?contest=2",
        )

        self.assertEqual(names, ["EMMA AGUILAR RIOS", "ANDREA AGOSTINI"])

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_participants_list_url(self, mock_get: Mock) -> None:
        payload = {
            "DataFields": ["BIB", "ID", "MostraNome"],
            "data": {
                "#1": [["12", "5", "Roman, Daniele"]],
                "#2": [["13", "6", "Bianchi, Sara"]],
            },
        }

        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://my4.raceresult.com/375451/participants/list?key=abc",
        )

        self.assertEqual(names, ["Daniele Roman", "Sara Bianchi"])
        self.assertEqual(mock_get.call_args.kwargs.get("headers"), RACERESULT_HEADERS)

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_participants_base_page(self, mock_get: Mock) -> None:
        config_payload = {
            "key": "abc",
            "server": "my4.raceresult.com",
            "TabConfig": {
                "Lists": [
                    {
                        "Name": "Online|Partecipanti_2",
                        "Mode": "",
                        "Contest": "0",
                        "Format": "VP",
                    },
                    {
                        "Name": "Hidden",
                        "Mode": "hidden",
                        "Contest": "0",
                        "Format": "VP",
                    },
                ]
            },
        }
        list_payload = {
            "DataFields": ["BIB", "ID", "MostraNome"],
            "data": {"#1": [["12", "5", "Roman, Daniele"]]},
        }

        config_response = Mock()
        config_response.raise_for_status.return_value = None
        config_response.json.return_value = config_payload

        list_response = Mock()
        list_response.raise_for_status.return_value = None
        list_response.json.return_value = list_payload

        mock_get.side_effect = [config_response, list_response]

        names = fetch_participants_from_url(
            "https://my.raceresult.com/375451/participants",
        )

        self.assertEqual(names, ["Daniele Roman"])
        self.assertEqual(
            mock_get.call_args_list[0].args[0],
            "https://my.raceresult.com/375451/participants/config",
        )
        self.assertEqual(
            mock_get.call_args_list[0].kwargs.get("params"), {"lang": "en"}
        )
        self.assertEqual(
            mock_get.call_args_list[1].args[0],
            "https://my4.raceresult.com/375451/participants/list",
        )
        params = mock_get.call_args_list[1].kwargs.get("params", {})
        self.assertEqual(params.get("key"), "abc")
        self.assertEqual(params.get("listname"), "Online|Partecipanti_2")
        self.assertEqual(params.get("page"), "participants")

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_participants_base_page_with_filter(
        self, mock_get: Mock
    ) -> None:
        config_payload = {
            "key": "abc",
            "server": "my4.raceresult.com",
            "TabConfig": {
                "Lists": [
                    {
                        "Name": "Online|Partecipanti_2",
                        "Mode": "",
                        "Contest": "0",
                        "Format": "VP",
                    },
                ]
            },
        }
        list_payload = {
            "DataFields": ["BIB", "ID", "MostraNome"],
            "data": {"#1": [["12", "5", "Roman, Daniele"]]},
            "groupFilters": [
                {"Type": 1, "Values": ["55K +3000", "24K +1400"]},
                {"Type": 2, "Values": ["Femminile", "Maschile"]},
                {"Type": 2, "Values": ["Under", "Over"]},
            ],
        }
        filtered_payload = {
            "DataFields": ["BIB", "ID", "MostraNome"],
            "data": {"#1": [["99", "9", "Bianchi, Sara"]]},
        }

        config_response = Mock()
        config_response.raise_for_status.return_value = None
        config_response.json.return_value = config_payload

        list_response = Mock()
        list_response.raise_for_status.return_value = None
        list_response.json.return_value = list_payload

        filtered_response = Mock()
        filtered_response.raise_for_status.return_value = None
        filtered_response.json.return_value = filtered_payload

        mock_get.side_effect = [config_response, list_response, filtered_response]

        names = fetch_participants_from_url(
            "https://my.raceresult.com/375451/participants",
            competition_name="55K +3000",
        )

        self.assertEqual(names, ["Sara Bianchi"])
        params = mock_get.call_args_list[2].kwargs.get("params", {})
        self.assertIn("f", params)
        self.assertIn("55K +3000", str(params.get("f")))

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_participants_base_page_no_filter_match(
        self, mock_get: Mock
    ) -> None:
        config_payload = {
            "key": "abc",
            "server": "my4.raceresult.com",
            "TabConfig": {
                "Lists": [
                    {
                        "Name": "Online|Partecipanti_2",
                        "Mode": "",
                        "Contest": "0",
                        "Format": "VP",
                    },
                ]
            },
        }
        list_payload = {
            "DataFields": ["BIB", "ID", "MostraNome"],
            "data": {"#1": [["12", "5", "Roman, Daniele"]]},
            "groupFilters": [
                {"Type": 1, "Values": ["55K +3000", "24K +1400"]},
            ],
        }

        config_response = Mock()
        config_response.raise_for_status.return_value = None
        config_response.json.return_value = config_payload

        list_response = Mock()
        list_response.raise_for_status.return_value = None
        list_response.json.return_value = list_payload

        mock_get.side_effect = [config_response, list_response]

        names = fetch_participants_from_url(
            "https://my.raceresult.com/375451/participants",
            competition_name="Unknown Distance",
        )

        self.assertEqual(names, ["Daniele Roman"])
        self.assertEqual(mock_get.call_count, 2)

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_dedup_and_accents(self, mock_get: Mock) -> None:
        payload = {
            "DataFields": ["BIB", "ID", "ucase([LFNAME])"],
            "data": {
                "#1_72K": {
                    "#2_M": [
                        ["1226", "1610", "FAIDHERBES, FRANÇOIS"],
                        ["1227", "1611", "Faidherbes, François"],
                        ["1228", "1612", "BÉLANGER, ANNE-SOPHIE"],
                    ]
                }
            },
        }

        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://my1.raceresult.com/364696/RRPublish/data/list?contest=2",
        )

        self.assertEqual(names, ["FRANÇOIS FAIDHERBES", "ANNE-SOPHIE BÉLANGER"])

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_error_payload(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"error": "list not found"}
        mock_get.return_value = response

        with self.assertRaisesRegex(ValueError, "RaceResult error: list not found"):
            fetch_participants_from_url(
                "https://my1.raceresult.com/364696/RRPublish/data/list?contest=8",
            )

    @patch("trailintel.participants.requests.get")
    def test_fetch_raceresult_only_for_matching_urls(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.headers = {"content-type": "application/json"}
        response.text = '{"participants": ["Alice Martin"]}'
        response.json.return_value = {"participants": ["Alice Martin"]}
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://my1.raceresult.com/364696/RRPublish/index.php?page=participants",
        )

        self.assertEqual(names, ["Alice Martin"])
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(
            mock_get.call_args.kwargs.get("headers"), GENERIC_BROWSER_HEADERS
        )

    @patch("trailintel.participants.requests.get")
    def test_fetch_wedosport_participants_with_competition_filter(
        self, mock_get: Mock
    ) -> None:
        html = """
        <table id="classifica">
          <thead>
            <tr>
              <th data-name="distanza">Distanza</th>
              <th data-name="pettorale">Pettorale</th>
              <th data-name="cognome">Cognome</th>
              <th data-name="nome">Nome</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Trail K50  50.10 KM, 2747m D+/D- <span style="display:none">gara 1</span></td>
              <td>12</td>
              <td>Achilea</td>
              <td>Marco</td>
            </tr>
            <tr>
              <td>Trail K30 30 KM</td>
              <td>44</td>
              <td>Rossi</td>
              <td>Giulia</td>
            </tr>
          </tbody>
        </table>
        """

        response = Mock()
        response.raise_for_status.return_value = None
        response.text = html
        response.headers = {"content-type": "text/html"}
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://www.wedosport.net/lista-iscritti/maremontana-trail-2026",
            competition_name="Trail K50 50.10 KM, 2747m D+/D-",
        )

        self.assertEqual(names, ["Marco Achilea"])
        self.assertEqual(
            mock_get.call_args.kwargs.get("headers"), GENERIC_BROWSER_HEADERS
        )

    @patch("trailintel.participants.requests.get")
    def test_fetch_grandraid_participants_follows_pagination(
        self, mock_get: Mock
    ) -> None:
        first_page = """
        <a id="pagination_inscrits" class="pagination_ancre"></a>
        <p><strong>2750 coureurs</strong></p>
        <ol class="result-list custom-result-list style-3">
          <li class="bold"><div><span class="title">Nom et prénom</span></div></li>
          <li><div><span class="title">Ammendola Antonio</span></div></li>
          <li><div><span class="title">ABAS Frédéric</span></div></li>
        </ol>
        <nav role="navigation" class="pagination">
          <ul class="pagination-items pagination_page_precedent_suivant">
            <li class="pagination-item next">
              <a href="/fr/listes-des-inscrits/?type_course=GRR&amp;debut_inscrits=50#pagination_inscrits">></a>
            </li>
          </ul>
        </nav>
        """
        second_page = """
        <ol class="result-list custom-result-list style-3">
          <li class="bold"><div><span class="title">Nom et prénom</span></div></li>
          <li><div><span class="title">ABITBOL BAPTISTE</span></div></li>
          <li><div><span class="title">Abrousse Jérôme</span></div></li>
        </ol>
        <nav role="navigation" class="pagination">
          <ul class="pagination-items pagination_page_precedent_suivant">
            <li class="pagination-item next disabled"><span class="pagination-item-label on">></span></li>
          </ul>
        </nav>
        """

        first_response = Mock()
        first_response.raise_for_status.return_value = None
        first_response.text = first_page

        second_response = Mock()
        second_response.raise_for_status.return_value = None
        second_response.text = second_page

        mock_get.side_effect = [first_response, second_response]

        names = fetch_participants_from_url(
            "https://www.grandraid-reunion.com/fr/listes-des-inscrits/?type_course=GRR",
        )

        self.assertEqual(
            names,
            [
                "Ammendola Antonio",
                "ABAS Frédéric",
                "ABITBOL BAPTISTE",
                "Abrousse Jérôme",
            ],
        )
        self.assertEqual(
            mock_get.call_args_list[0].kwargs.get("headers"), GENERIC_BROWSER_HEADERS
        )
        self.assertEqual(
            mock_get.call_args_list[1].args[0],
            "https://www.grandraid-reunion.com/fr/listes-des-inscrits/?type_course=GRR&debut_inscrits=50",
        )

    @patch("trailintel.participants.requests.get")
    def test_fetch_grandraid_participants_only_for_matching_urls(
        self, mock_get: Mock
    ) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.headers = {"content-type": "text/html"}
        response.text = "<ul><li class='participant'>Alice Martin</li></ul>"
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://www.grandraid-reunion.com/fr/infos-pratiques/",
        )

        self.assertEqual(names, ["Alice Martin"])
        self.assertEqual(mock_get.call_count, 1)

    @patch("trailintel.participants.requests.get")
    def test_fetch_endu_participants_json_url_paginates_and_filters(
        self, mock_get: Mock
    ) -> None:
        first_payload = {
            "total": "2",
            "records": "3",
            "page": "1",
            "rows": [
                {
                    "id": 0,
                    "cell": [
                        "",
                        "Jesus Aaron",
                        "Alonso Herrera",
                        "1980",
                        "ESP",
                        "RUN CARD",
                        "",
                        "52 k Saturday 2 May - Athletes with Fidal Runcard",
                    ],
                },
                {
                    "id": 1,
                    "cell": [
                        "",
                        "Maria Luisa",
                        "Acerbi",
                        "1987",
                        "ITA",
                        "BIKE & RUN",
                        "",
                        "26 k Saturday 2 May - Athletes with Fidal Runcard",
                    ],
                },
            ],
        }
        second_payload = {
            "total": "2",
            "records": "3",
            "page": "2",
            "rows": [
                {
                    "id": 2,
                    "cell": [
                        "",
                        "Marco",
                        "Amigoni",
                        "1976",
                        "ITA",
                        "RUN CARD",
                        "",
                        "52 k NON competitive race Saturday 2 May",
                    ],
                }
            ],
        }

        first_response = Mock()
        first_response.raise_for_status.return_value = None
        first_response.json.return_value = first_payload

        second_response = Mock()
        second_response.raise_for_status.return_value = None
        second_response.json.return_value = second_payload

        mock_get.side_effect = [first_response, second_response]

        names = fetch_participants_from_url(
            "https://event.endu.net/events/event/entrants-json?idevento=100384&amp;idgara=0&_search=false&rows=20&page=1",
            competition_name="52k",
        )

        self.assertEqual(names, ["Jesus Aaron Alonso Herrera", "Marco Amigoni"])
        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(
            mock_get.call_args_list[0].args[0],
            "https://event.endu.net/events/event/entrants-json",
        )
        first_params = mock_get.call_args_list[0].kwargs.get("params", {})
        self.assertEqual(first_params.get("idevento"), "100384")
        self.assertEqual(first_params.get("idgara"), "0")
        self.assertEqual(first_params.get("rows"), "1000")
        self.assertEqual(first_params.get("page"), "1")
        self.assertEqual(mock_get.call_args_list[0].kwargs.get("headers"), ENDU_HEADERS)
        second_params = mock_get.call_args_list[1].kwargs.get("params", {})
        self.assertEqual(second_params.get("page"), "2")

    @patch("trailintel.participants.requests.get")
    def test_fetch_endu_participants_page_url_builds_json_endpoint(
        self, mock_get: Mock
    ) -> None:
        payload = {
            "total": "1",
            "records": "1",
            "page": "1",
            "rows": [
                {
                    "id": 0,
                    "cell": [
                        "",
                        "Gabriele",
                        "Abelli",
                        "1968",
                        "ITA",
                        "RUN CARD",
                        "",
                        "16 k Sunday 3 May - Athletes with Fidal Runcard",
                    ],
                }
            ],
        }

        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload
        mock_get.return_value = response

        names = fetch_participants_from_url(
            "https://event.endu.net/events/event/entrants?editionId=100384",
        )

        self.assertEqual(names, ["Gabriele Abelli"])
        self.assertEqual(
            mock_get.call_args.args[0],
            "https://event.endu.net/events/event/entrants-json",
        )
        params = mock_get.call_args.kwargs.get("params", {})
        self.assertEqual(params.get("idevento"), "100384")
        self.assertEqual(params.get("idgara"), "0")
        self.assertEqual(params.get("rows"), "1000")


if __name__ == "__main__":
    unittest.main()
