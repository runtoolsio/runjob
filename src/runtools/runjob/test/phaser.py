from runtools.runjob.phaser import RunContext


class FakeRunContext(RunContext):

    def __init__(self):
        self.output = []

    @property
    def task_tracker(self):
        return None

    def new_output(self, output, is_err=False):
        self.output.append((output, is_err))
