from dataclasses import dataclass
from inquirer import prompt, List
from inquirer.themes import load_theme_from_dict

from settings import CANCEL_ORDERS, FUTURE_ACTIONS


@dataclass
class Mode:
    soft_id: int
    text: str
    type: str
    is_new: bool = False
    is_numeric: bool = True

    def __str__(self) -> str:
        return ("⭐️ NEW | " if self.is_new else "") + self.text


def choose_mode():
    def ask_question(question: str, modes: list):
        total_numerics = 0
        choices = []
        for mode in modes:
            mode_numeric = ""
            if mode.is_numeric:
                total_numerics += 1
                mode_numeric = f"{total_numerics}. "

            choices.append((f"{mode_numeric}{mode}", mode.soft_id))

        questions = [
            List(
                name='custom_question',
                message=question,
                choices=choices,
                carousel=True,
            )
        ]

        raw_answer = prompt(
            questions=questions,
            raise_keyboard_interrupt=True,
            theme=THEME,
        )
        return next((mode for mode in modes if mode.soft_id == raw_answer['custom_question']))

    orders_positions_str = " & ".join([k.title() for k, v in CANCEL_ORDERS.items() if v])
    if orders_positions_str:
        cancel_mode_name = f"Cancel All {orders_positions_str}"
    else:
        cancel_mode_name = f"Cancel Nothing"

    futures_sides = "/".join([k for k, v in FUTURE_ACTIONS.items() if v])

    answer = ask_question(
        question="🚀 Choose mode",
        modes=[
            Mode(soft_id=0, type="", text="(Re)Create Database", is_numeric=False),
            Mode(soft_id=1, type="module", text=f"Futures Limits ({futures_sides})"),
            Mode(soft_id=4, type="module", text=f"Pair Futures Limits (Delta-Neutral)"),
            Mode(soft_id=2, type="module", text=cancel_mode_name),
            Mode(soft_id=3, type="module", text="Parse"),
        ]
    )

    if answer.soft_id == 0:
        answer = ask_question(
            question="💾 Which type of Database you want to create?",
            modes=[
                Mode(soft_id=-1, type="", text="← Exit", is_numeric=False),
                Mode(soft_id=101, type="database",  text="Create new single database", is_numeric=False),
                Mode(soft_id=102, type="database",  text="Create new groups database (for delta-neutral)", is_numeric=False),
            ]
        )

    return answer


THEME = load_theme_from_dict({"List": {
    "selection_cursor": "👉🏻",
    # "selection_color": "violetred1",
}})
