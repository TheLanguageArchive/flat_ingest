<?php

namespace Drupal\flat_ingest\Drush\Commands;

use Drush\Commands\DrushCommands;
use Drupal\node\Entity\Node;
use Drupal\file\Entity\File;
use Drupal\media\Entity\Media;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;

/**
 * Drush commands for creating Islandora entities in bulk.
 */
class IslandoraIngestCommands extends DrushCommands {

  protected $termCache = [];

  /**
   * Bulk import nodes, files, and media from a JSON spec file.
   *
   * @command flat:bulk-import
   * @param string $json_file
   *   Path to the JSON spec file.
   * @usage drush flat:bulk-import batch.json
   *   Import entities defined in batch.json.
   */
  public function bulkImport($json_file) {
    if (!file_exists($json_file)) {
      throw new FileNotFoundException("JSON file not found: $json_file");
    }

    $data = json_decode(file_get_contents($json_file), TRUE);
    if (!$data) {
      throw new \RuntimeException("Invalid JSON in file: $json_file");
    }

    if (!isset($data['operations'])) {
      throw new \RuntimeException("JSON must contain 'operations' array.");
    }

    $results = [
      'processed' => [],
      'errors' => [],
      'stats' => [
        'create_node' => ['count' => 0, 'total_time' => 0],
        'update_node' => ['count' => 0, 'total_time' => 0],
        'create_file' => ['count' => 0, 'total_time' => 0],
        'create_media' => ['count' => 0, 'total_time' => 0],
        'total_operations' => 0,
        'total_time' => 0,
      ],
    ];

    $entityIdMap = [];
    $entityUuidMap = [];

    $entityTypeManager = \Drupal::entityTypeManager();
    $start_time = microtime(TRUE);

    foreach ($data['operations'] as $op) {
      $op_start_time = microtime(TRUE);
      try {
        switch ($op['op']) {
          case 'create_node':
            $node = Node::create([
              'type' => 'islandora_object',
            ]);
            $node->set('title', $op['title']);
            $node->set('field_pid', $op['pid']);
            $modelTid = $this->getTermTid($op['model_uuid']);
            $node->set('field_model', ['target_id' => $modelTid]);

            $parentId = NULL;
            if (isset($op['parent_uuid'])) {
              $parents = $entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $op['parent_uuid']]);
              if ($parents) {
                $parentId = reset($parents)->id();
              }
            } elseif (isset($op['parent_temp_id'])) {
              $parentId = $entityIdMap[$op['parent_temp_id']] ?? NULL;
            }
            if ($parentId) {
              $node->set('field_member_of', ['target_id' => $parentId]);
            }

            $node->save();
            $entityIdMap[$op['temp_id']] = $node->id();
            $entityUuidMap[$op['temp_id']] = $node->uuid();
            $results['processed'][] = [
              'temp_id' => $op['temp_id'],
              'uuid' => $node->uuid(),
              'vid' => $node->getRevisionId(),
            ];
            $results['stats']['create_node']['count']++;
            $results['stats']['create_node']['total_time'] += (microtime(TRUE) - $op_start_time);
            break;

          case 'update_node':
            $uuid = $entityUuidMap[$op['temp_id']] ?? NULL;
            if (!$uuid) {
              throw new \Exception("UUID not found for temp_id {$op['temp_id']}");
            }
            $nodes = $entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $uuid]);
            if (!$nodes) {
              throw new \Exception("Node not found for UUID $uuid");
            }
            $node = reset($nodes);
            $node->setNewRevision(TRUE);
            $node->set('title', $op['title']);
            $node->set('field_pid', $op['pid']);
            $modelTid = $this->getTermTid($op['model_uuid']);
            $node->set('field_model', ['target_id' => $modelTid]);

            $parentId = NULL;
            if (isset($op['parent_uuid'])) {
              $parents = $entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $op['parent_uuid']]);
              if ($parents) {
                $parentId = reset($parents)->id();
              }
            } elseif (isset($op['parent_temp_id'])) {
              $parentId = $entityIdMap[$op['parent_temp_id']] ?? NULL;
            }
            if ($parentId !== NULL) {
              $node->set('field_member_of', ['target_id' => $parentId]);
            }

            $node->save();
            $results['processed'][] = [
              'temp_id' => $op['temp_id'],
              'uuid' => $node->uuid(),
              'vid' => $node->getRevisionId(),
            ];
            $results['stats']['update_node']['count']++;
            $results['stats']['update_node']['total_time'] += (microtime(TRUE) - $op_start_time);
            break;

          case 'create_file':
            $file = File::create([
              'filename' => $op['filename'],
              'uri' => $op['uri'],
              'filemime' => $op['filemime'],
            ]);
            $file->setPermanent();
            $file->save();
            $entityIdMap[$op['temp_id']] = $file->id();
            $entityUuidMap[$op['temp_id']] = $file->uuid();
            $results['processed'][] = [
              'temp_id' => $op['temp_id'],
              'uuid' => $file->uuid(),
            ];
            $results['stats']['create_file']['count']++;
            $results['stats']['create_file']['total_time'] += (microtime(TRUE) - $op_start_time);
            break;

          case 'create_media':
            $media = Media::create([
              'bundle' => $op['bundle'],
            ]);
            $media->set('name', $op['name']);
            $useTid = $this->getTermTid($op['media_use_uuid']);
            $media->set('field_media_use', ['target_id' => $useTid]);

            $fileId = NULL;
            if (isset($op['file_uuid'])) {
              $files = $entityTypeManager->getStorage('file')->loadByProperties(['uuid' => $op['file_uuid']]);
              if ($files) {
                $fileId = reset($files)->id();
              }
            } elseif (isset($op['file_temp_id'])) {
              $fileId = $entityIdMap[$op['file_temp_id']] ?? NULL;
            }
            if (!$fileId) {
              throw new \Exception("File not found for media {$op['temp_id']}");
            }
            $media->set($op['relation_field'], ['target_id' => $fileId]);

            $nodeId = NULL;
            if (isset($op['node_uuid'])) {
              $nodes = $entityTypeManager->getStorage('node')->loadByProperties(['uuid' => $op['node_uuid']]);
              if ($nodes) {
                $nodeId = reset($nodes)->id();
              }
            } elseif (isset($op['node_temp_id'])) {
              $nodeId = $entityIdMap[$op['node_temp_id']] ?? NULL;
            }
            if (!$nodeId) {
              throw new \Exception("Node not found for media {$op['temp_id']}");
            }
            $media->set('field_media_of', ['target_id' => $nodeId]);

            $media->save();
            $entityIdMap[$op['temp_id']] = $media->id();
            $entityUuidMap[$op['temp_id']] = $media->uuid();
            $results['processed'][] = [
              'temp_id' => $op['temp_id'],
              'uuid' => $media->uuid(),
            ];
            $results['stats']['create_media']['count']++;
            $results['stats']['create_media']['total_time'] += (microtime(TRUE) - $op_start_time);
            break;

          default:
            throw new \Exception("Unknown operation: {$op['op']}");
        }
      }
      catch (\Exception $e) {
        $results['errors'][] = [
          'op' => $op['op'],
          'temp_id' => $op['temp_id'] ?? 'unknown',
          'message' => $e->getMessage(),
        ];
      }
      $results['stats']['total_operations']++;
    }

    $results['stats']['total_time'] = microtime(TRUE) - $start_time;

    // Calculate average times
    foreach (['create_node', 'update_node', 'create_file', 'create_media'] as $op_type) {
      if ($results['stats'][$op_type]['count'] > 0) {
        $results['stats'][$op_type]['avg_time'] =
          $results['stats'][$op_type]['total_time'] / $results['stats'][$op_type]['count'];
      } else {
        $results['stats'][$op_type]['avg_time'] = 0;
      }
    }

    // Print JSON output so Python can consume it.
    return json_encode($results, JSON_PRETTY_PRINT);
  }

  /**
   * Get TID for a taxonomy term UUID, with caching.
   */
  protected function getTermTid($uuid) {
    if (isset($this->termCache[$uuid])) {
      return $this->termCache[$uuid];
    }
    $terms = \Drupal::entityTypeManager()->getStorage('taxonomy_term')->loadByProperties(['uuid' => $uuid]);
    if (!$terms) {
      throw new \Exception("Taxonomy term not found for UUID $uuid");
    }
    $term = reset($terms);
    $this->termCache[$uuid] = $term->id();
    return $this->termCache[$uuid];
  }

}