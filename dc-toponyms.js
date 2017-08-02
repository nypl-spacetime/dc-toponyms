const fs = require('fs')
const path = require('path')
const H = require('highland')
const R = require('ramda')
const got = require('got')

const url = 'http://brick-by-brick.herokuapp.com/tasks/select-toponym/submissions/all.ndjson'
const filename = 'brick-by-brick-submissions.ndjson'

const SURVEYOR_DATASET = 'surveyor'

function download (config, dirs, tools, callback) {
  got.stream(url)
    .pipe(fs.createWriteStream(path.join(dirs.current, filename)))
    .on('finish', callback)
}

function getPhotoId (item) {
  return `${item.organization.id}-${item.item.id}`
}

function transform (config, dirs, tools, callback) {
  H(fs.createReadStream(path.join(dirs.download, filename)))
    .split()
    .compact()
    .map(JSON.parse)
    .map((item) => {
      const toponyms = R.flatten(item.submissions.map((submission) => submission.steps
        .filter((step) => step.data.toponym)))
        .map((step) => step.data.toponym)

      if (!toponyms.length) {
        return
      }

      return [
        {
          type: 'object',
          obj: {
            id: getPhotoId(item),
            type: 'st:Photo',
            name: toponyms[0],
          }
        },
        {
          type: 'relation',
          obj: {
            from: getPhotoId(item),
            to: `${SURVEYOR_DATASET}/${getPhotoId(item)}`,
            type: 'st:sameAs'
          }
        }
      ]
    })
    .flatten()
    .compact()
    .map(H.curry(tools.writer.writeObject))
    .nfcall([])
    .series()
    .stopOnError(callback)
    .done(callback)
}

// ==================================== API ====================================

module.exports.steps = [
  download,
  transform
]
