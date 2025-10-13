import load from '@commitlint/load';
import lint from '@commitlint/lint';

const minPRDescriptionLength = 10;

// Utility functions
const processReport = (type, report, warnOnly = false) => {
    if (report.warnings.length > 0) {
        warn(
            `${type} '${report.input}': ${report.warnings
                .map((w) => w.message)
                .join(', ')}`,
        );
    }

    if (report.errors.length > 0) {
        const reportFn = warnOnly ? warn : fail;

        reportFn(
            `${type} '${report.input}': ${report.errors
                .map((e) => e.message)
                .join(', ')}`,
        );
    }

    return report.valid || warnOnly ? Promise.resolve() : Promise.reject();
};

const reportCommitMessage = (report) => processReport('Commit Message', report, true);

const reportPRTitle = (report) => processReport('PR Title', report, false);

const lintMessage = (message, opts, reporter) =>
    lint(
        message,
        opts.rules,
        opts.parserPreset ? {parserOpts: opts.parserPreset.parserOpts} : {},
    ).then(reporter);

const pr = danger.github.pr;

// check commit messages and PR name
schedule(
    Promise.all([
        load({}, {file: './.commitlintrc.json', cwd: process.cwd()}).then(
            (opts) =>
                Promise.all(
                    danger.git.commits
                        .map((c) => c.message)
                        .map((m) => lintMessage(m, opts, reportCommitMessage)),
                )
                    .catch(() =>
                        markdown(
                            '> All commits should follow ' +
                            '[Conventional commits](https://cheatography.com/albelop/cheat-sheets/conventional-commits). ' +
                            'It seems some of the commit messages are not following those rules, please fix them.',
                        ),
                    )
                    .then(() => lintMessage(pr.title, opts, reportPRTitle))
                    .catch(() =>
                        markdown(
                            '> Pull request title should follow ' +
                            '[Conventional commits](https://cheatography.com/albelop/cheat-sheets/conventional-commits).',
                        ),
                    ),
        ),
        new Promise((resolve, reject) => {
            // No PR is too small to include a description of why you made a change
            if (pr.body.length < minPRDescriptionLength) {
                warn(`:exclamation: Please include a description of your PR changes.`);

                markdown(
                    '> Pull request should have a description of the underlying changes.',
                );
            }
        }),
    ]),
);
